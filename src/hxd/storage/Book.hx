package hxd.storage;
import tink.runloop.Task;
import tink.runloop.*;
import tink.concurrent.Thread;
using Lambda;

import tink.streams.Stream;
import hxd.storage.*;
import haxe.io.Bytes;
import tink.RunLoop;
import hxd.sys.Engine;
import sys.io.*;
import sys.*;
import tink.CoreApi;
import tink.core.Error.ErrorCode;

using hxd.storage.Serializer;

enum CreateResult {
	Created(page:Page, records:Array<Record>);
	Expect(count:Int);
}

enum CreateHint {
	FullPage;
	PartialPage;
}

typedef IoWorkingGroup = {
	var cache:Worker;
	var allocate:Worker;
	var write:Worker;
}

typedef OutputGroup = {
	var update:FileOutput;
	var append:FileOutput;
}

class Book {
	var name:String;
	var file(get, never):String;

	public var pageSize(default, null):Int;

	var retention:Map<Int, Page>;
	var workers:IoWorkingGroup;
	var _writer:OutputGroup;
	var writer(get, never):OutputGroup;

	function get_writer() {
		ensure();
		if (_writer == null)
			_writer = {update: File.update(file, true), append: File.append(file, true)};
		else if (_writer.update == null)
			_writer.update = File.update(file, true);
		else if (_writer.append == null)
			_writer.append = File.append(file, true);
		return _writer;
	}

	var _reader:FileInput;
	var reader(get, never):FileInput;

	function get_reader() {
		ensure();
		if (_reader == null)
			_reader = File.read(file, true);
		return _reader;
	}

	function new(n:String) {
		name = n;
		pageSize = Engine.config.book.pageSize;
		retention = new Map<Int, Page>();
		_sizes = [];
		workers = {
			allocate: RunLoop.current.createSlave(),
			cache: RunLoop.current.createSlave(),
			write: RunLoop.current.createSlave()
		};
	}

	function get_file() {
		return './${Engine.path}/$name.hxbk';
	}
	
	var _stat:FileStat;
	var stat(get, never):FileStat;

	function get_stat() {
		ensure();
		if (_stat == null)
			_stat = FileSystem.stat(file);
		return _stat;
	}

	var size(get, never):Int;

	function get_size() {
		return stat.size;
	}

	var pages(get, never):Int;

	function get_pages() {
		return Math.ceil(size / pageSize);
	}

	var _sizes:Array<Int>;
	var sizes(get, never):Array<Int>;

	function get_sizes() {
		if (_sizes == null || _sizes.length == 0)
			_sizes = peek();
		return _sizes;
	}

	var serializationHooks:Array<Bytes->Bytes> = [];
	var deserializationHooks:Array<Bytes->Bytes> = [];

	@:noCompletion public function postSerialization(_b:Bytes) {
		var b = _b;
		for (hook in serializationHooks) {
			b = hook(b);
		}
		return b;
	}

	@:noCompletion public function preDeserialization(_b:Bytes) {
		var b = _b;
		for (hook in deserializationHooks) {
			b = hook(b);
		}
		return b;
	}

	public function addStoragePlan(serialize:Bytes->Bytes, deserialize:Bytes->Bytes) {
		serializationHooks.push(serialize);
		deserializationHooks.insert(0, deserialize);
	}

	function ensure() {
		Engine.ensure();
		if (!FileSystem.exists(file)) {
			var output = File.write(file, true);
			output.writeString('');
			output.flush();
			output.close();
		}
	}

	public static function open(name:String) {
		var book = @:privateAccess new Book(name);
		book.ensure();
		return book;
	}

	function peek() {
		var retVal = [];
		for (i in 0...pages) {
			reader.seek(i * pageSize, FileSeek.SeekBegin);
			retVal.push(reader.readInt16());
		}
		return retVal;
	}

	function cleanse() {
		var dirty = [];
		var retent = retention.array();
		for (page in retent) {
			// var page = retention.get(pageNo);
			if (page.dirty) {
				dirty.push(page);
				retention.remove(page.number.value);
			}
		}
		return dirty;
	}

	public function calculatePadding() {
		var sizes = peek();
		var total = 0;
		for (size in sizes) {
			total += size;
		}
		var frac = (size - total) / size;
		return {bytes: total, percentage: frac};
	}

	public function count() {
		var worker = RunLoop.current.createSlave();
		var done = Future.trigger();
		var total = 0;
		var futures = [];
		for (i in 0...pages) {
			var done = Future.trigger();
			worker.work(() -> {
				var page = read(i);
				var count = page.records.count();
				// trace('Done counting page: $count (${futures.length})');
				total += count;
				done.trigger(Noise);
				// trace('page-$i count: $count (total: $total)');
			});
			futures.push(done.asFuture());
			
		}
		Future.ofMany(futures).handle(_ -> {
			trace("Count: " + total);
			done.trigger(total);
			worker.kill();
		});
		return done.asFuture();
	}

	function read(pageNo:Int):Page {
		// trace('Reading $pageNo');
		if (retention.exists(pageNo)) {
			// trace("From retention");
			var page = retention.get(pageNo);
			@:privateAccess page.number = pageNo;
			return page;
		}
		reader.seek(pageNo * pageSize, FileSeek.SeekBegin);
		var pSize = 0;
		try {
			pSize = reader.readInt16();
		} catch (e:Dynamic) {
			var newPage = new Page(this);
			// trace('Error: $e');
			retention.set(pageNo, newPage);
			pSize = -1;
		}
		if (pSize == 0) {
			// trace('read blank page.');
			var newPage = new Page(this);
			retention.set(pageNo, newPage);
		} else {
			var pageBytes = reader.read(pSize);
			try {
				var page:Page = Serializer.deserialize(pageBytes, preDeserialization);
				@:privateAccess page.book = this;
				@:privateAccess page.number = pageNo;
				retention.set(pageNo, page);
			} catch (e:Dynamic) {
				var newPage = new Page(this);
				// trace('Error: $e');
				retention.set(pageNo, newPage);
			}
		}
		return retention.get(pageNo);
	}

	function write(page:Page) {
		// trace('Begin write: ${page.number}, ${page.records.count()}');
		var done = Future.trigger();
		if(page.size > pageSize) {
			// trace('Sub create: ${page.size}/$pageSize');
			var results = [];
			create(page.records.array()).forEach(result -> {
				results.push(result);
				return Resume;
			}).handle(() -> {
				var futures = [];
				results.iter(result -> {
					switch(result) {
						case Created(page, records):
							futures.push(write(page));
						default:
					}
					return;
				});
				Future.ofMany(futures).handle(_ -> {
					
					done.trigger(Noise);
				});
			});
		} else {

			// trace("Safe write");
			workers.write.work(() -> {
				// trace("Begin writing.");
				var isNew = page.number.value == -1;
				if (isNew) {
					@:privateAccess page.number.value = pages;
					var append = File.append(file, true);
					append.write(Bytes.alloc(pageSize));
					append.flush();
					append.close();
					// trace("Close appender.");
					// _sizes.push(page.size);
				}
				trace("Writing: " + page.number);
				var update = File.update(file, true);
				update.seek(page.number.value * pageSize, FileSeek.SeekBegin);
				update.writeInt16(page.size);
				update.write(page.bytes);
				// update.write(Bytes.ofString("END"));
				// trace(page.bytes.toString());
				update.flush();
				update.close();
				// trace("Flushed and closed...");
				_stat = null;
				_sizes = null;
				// if (!isNew)
				// 	_sizes[page.number.value] = page.size;
				// trace('Done writing ${page.number} ${page.records.count()} records');
				done.trigger(Noise);
			});
		}
		return done.asFuture();
	}

	function close() {
		trace("Closing...");

		writer.update.close();
	}

	function refresh() {
		_writer = null;
		_reader = null;
	}

	public function commit() {
		// trace('Begin commit.');
		var futures = [];
		var pages = [];
		var retent = retention.array();
		for (page in retent) {
			
				futures.push(write(page));
			pages.push(page.number);
			
		}
		// trace('Waiting for ${futures.length} results');
		var done = Future.trigger();
		Future.ofMany(futures).handle(_ -> {
			cleanse();
			// close();
			trace(pages);
			done.trigger(Noise);
			// trace("Done...?");
		});
		return done.asFuture();
	}

	public function create(records:Array<Record>):SignalStream<CreateResult, Error> {
		var a = Signal.trigger();
		var stream = new SignalStream(a.asSignal());
		var estSize = records.serialize(postSerialization).length * 2;
		var segments = [];
		var segment = [];
		var counter = 0;
		var overgrowthFactor = Math.ceil(estSize / pageSize);
		var segSize = Math.ceil(records.length / overgrowthFactor);
		var futures = [];
		var fulfillment:Ref<Int> = 0;
		var expected:Ref<Int> = 0;
		for (record in records) {
			segment.push(record);
			counter++;
			if (counter == segSize) {
				counter = 0;
				segments.push(segment);
				if(segment.length != 0) futures.push(safeCreate(segment, a.trigger, fulfillment, expected));
				expected.value = futures.length;
				segment = [];
			}
			if (record == records[records.length - 1]) {
				segments.push(segment);
				if(segment.length != 0)  futures.push(safeCreate(segment, a.trigger, fulfillment, expected));
				expected.value = futures.length;
			}
			
		}
		
		trace("Expected: " + expected);
		trace("Fulfillment: " + fulfillment);
		a.trigger(Data(Expect(futures.length)));
		// trace('Waiting on ${futures.length} futures');
		// Future.ofMany(futures).handle(_ -> {
		// 	trace("Done creating");
		// 	a.trigger(End);
		// });
		return stream;
	}

	function safeCreate(records:Array<Record>, emit:Callback<Yield<CreateResult, Error>>, fulfillment:Ref<Int>, expected:Ref<Int>):Future<Noise> {
		// trace('Scheduling safe create: fulfillment: $fulfillment, expected: $expected');
		var done = Future.trigger();
		workers.cache.work(() -> {

		var doEmit = val -> {
			fulfillment.value += 1;
			emit.invoke(val);
			// trace("Emit");
			if(fulfillment.value == expected.value && expected.value != 0) {
				trace('Reached end: $fulfillment, $expected');
				emit.invoke(End);
			}
			done.trigger(Noise);
			return;

		}
			
			// trace('Safe create: ${records.length}');
			var newPage = () -> {
				var newPage = new Page(this);
				// trace("New page");
				var done = Future.trigger();
				workers.cache.work(() -> {
					if (newPage.create(records)) {
						// trace("Working new page");
						
							var retentionId = -retention.count();
							while(retention.exists(retentionId)) {
								retentionId--;
							}
							// trace('${records.length} records fit into ${newPage.number}');
							retention.set(retentionId, newPage);
							doEmit(Data(Created(newPage, records)));
							done.trigger(Noise);
					} else {
						throw "This shouldn't happen.";
					}
					return;
				});
				return done.asFuture();
				
				
			};
			// if(hint == FullPage) return newPage();
			
			var recordSize = records.serialize(postSerialization).length;
			var maxSpace = pageSize;
			
			

				var retent = retention.array();
				var knownSizes = new Map<Int,Int>();
				for(page in retent) {
					
					var size = page.size;
					// trace('$recordSize + $size > $pageSize');
					if(recordSize + size > pageSize) continue;
					knownSizes.set(page.number.value, size);
					if(size < maxSpace) { if(page.create(records)) {
						// retention.set(page.number.value, page);
						
						doEmit(Data(Created(page, records)));
						done.trigger(Noise);
						return Noise;
					} else {
						maxSpace = size;
					}
					
				}
				}
				for(pageNo in 0...pages) {
					if(retention.exists(pageNo)) continue;
					if(knownSizes.exists(pageNo) && knownSizes.get(pageNo) > maxSpace) continue;
					var page = read(pageNo);
					var size = page.size;
					if(recordSize + size > pageSize) continue;
					// trace('From disk: ${page.number}');
					if(size < maxSpace){

						if(page.create(records)) {
							// retention.set(pageNo, page);
							doEmit(Data(Created(page, records)));
							done.trigger(Noise);
							return Noise;
						} else {
							maxSpace = size;
						}
					}
				}
				newPage().handle(() -> {
					done.trigger(Noise);
				});
			return Noise;
		});
		return done.asFuture();
	}
}
