package hxd.storage;

import tink.runloop.*;

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
		Engine.ensure();
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
		for (pageNo in retention.keys()) {
			var page = retention.get(pageNo);
			if (page.dirty) {
				dirty.push(page);
				retention.remove(pageNo);
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
			worker.work(() -> {
				var page = read(i);
				var count = page.records.count();
				total += count;
				trace('page-$i count: $count (total: $total)');
			});
			futures.push(RunLoop.current.delegate(Noise, worker));
		}
		Future.ofMany(futures).handle(_ -> {
			done.trigger(total);
			worker.kill();
		});
		return done.asFuture();
	}

	function read(pageNo:Int):Page {
		if (retention.exists(pageNo)) {
			trace("From retention");
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
			trace('Error: $e');
			retention.set(pageNo, newPage);
			pSize = -1;
		}
		if (pSize == 0) {
			trace('read blank page.');
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
				trace('Error: $e');
				retention.set(pageNo, newPage);
			}
		}
		return retention.get(pageNo);
	}

	function write(page:Page) {
		// trace('Begin write: ${page.number}');
		workers.write.work(() -> {
			var isNew = page.number.value == -1;
			if (isNew) {
				@:privateAccess page.number.value = pages;
				writer.append = File.append(file, true);
				writer.append.write(Bytes.alloc(pageSize));
				writer.append.flush();
				writer.append.close();
				_sizes.push(page.size);
			}
			writer.update.seek(page.number.value * pageSize, FileSeek.SeekBegin);
			writer.update.writeInt16(page.size);
			writer.update.write(page.bytes);
			writer.update.flush();
			_stat = null;
			if (!isNew)
				_sizes[page.number.value] = page.size;
		});
		return RunLoop.current.delegate(Noise, workers.write);
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
		trace('Begin commit.');
		var futures = [];
		for (pageNo in retention.keys()) {
			var page = retention.get(pageNo);
			if (page.dirty) {
				futures.push(write(page));
			}
		}
		var done = Future.trigger();
		Future.ofMany(futures).handle(_ -> {
			cleanse();
			close();
			done.trigger(Noise);
		});
		return done.asFuture();
	}

	public function create(records:Array<Record>):SignalStream<CreateResult, Error> {
		var a = Signal.trigger();
		var stream = new SignalStream(a.asSignal());
		var estSize = records.serialize().length;
		var segments = [];
		var segment = [];
		var counter = 0;
		var overgrowthFactor = Math.ceil(estSize / pageSize);
		var segSize = Math.ceil(records.length / overgrowthFactor);
		for (record in records) {
			segment.push(record);
			counter++;
			if (counter == segSize) {
				counter = 0;
				segments.push(segment);
				segment = [];
			}
			if (record == records[records.length - 1]) {
				segments.push(segment);
			}
		}
		a.trigger(Data(Expect(segments.length)));
		var futures = [];
		for (segment in segments) {
			futures.push(safeCreate(segment, a.trigger, segment.length == segSize && segments.length > 1 ? FullPage : PartialPage));
		}
		Future.ofMany(futures).handle(_ -> {
			a.trigger(End);
		});
		return stream;
	}

	function safeCreate(records:Array<Record>, emit:Callback<Yield<CreateResult, Error>>, hint = PartialPage, last = false) {
		trace('HINT: $hint');
		var estRecordSize = records.serialize(postSerialization).length;
		var newPage = () -> {
			var newPage = new Page(this);
			trace("New page");
			workers.allocate.work(() -> {
				if (newPage.create(records)) {
					trace("Working new page");
					retention.set(-retention.count(), newPage);
					emit.invoke(Data(Created(newPage, records)));
					if (last)
						emit.invoke(End);
				} else {
					throw new Error(InternalError, "This should never happen.");
				}
			});
			return RunLoop.current.delegate(Noise, workers.allocate);
		};
		switch (hint) {
			case FullPage:
				return newPage();
			case PartialPage:
				trace('$sizes, $estRecordSize, $pages, $size');
				if(retention.count() != 0) {
					for(key in retention.keys()) {
						var page = retention[key];
						if(page.size + estRecordSize < pageSize) {
							if(page.create(records)) {
								emit.invoke(Data(Created(page, records)));
								return Noise;
							}
						}
					}
				}
				if (size > pageSize) {
					for (pageNo in 0...pages) {
						trace('check page');
						if (sizes[pageNo] + estRecordSize < pageSize) {
							workers.cache.work(() -> {
								trace("Begin readnig into memory");
								var page = read(pageNo);
								trace('Read into memory: ${page.number}');
								if (page.create(records)) {
									retention.set(page.number.value, page);
									emit.invoke(Data(Created(page, records)));
									if (last)
										emit.invoke(End);
									trace("Done");
								} else {
									throw new Error(InternalError, "This should also never happen.");
								}
							});
							return RunLoop.current.delegate(Noise, workers.cache);
						}
					}
				}
				return newPage();
		}
	}
}
