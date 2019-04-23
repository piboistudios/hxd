package;

using Lambda;

import tink.streams.*;
import tink.core.Error.ErrorCode;
import hxd.storage.*;
import hxd.sys.Engine;
import tink.CoreApi;
import tink.unit.*;
import tink.streams.Stream;
import tink.testrunner.*;

// import tink.unit.Assert.assert;
class Main {
	public static function main() {
		Runner.run(TestBatch.make([new TestStorage()])).handle(Runner.exit);
	}
}

@:timeout(1200000)
@:asserts
class TestStorage {
	public function new() {}

	// @:exclude
	// @:variant(10)
	// @:variant(20)
	// @:variant(30)
	// @:variant(40)
	// @:varaint(50)
	// @:variant(60)
	// @:variant(70)
	// @:variant(80)
	// @:variant(90)
	// @:variant(100)
	// @:variant(200)
	// @:variant(500)
	// @:varaint(1000)
	// @:variant(2000)
	// @:variant(5000)
	// @:variant(50000)
	// @:variant(100000)
	// @:variant(10)
	@:variant(20000)
	@:variant(200000)
	@:variant(1000000)
	
	
	// @:variant(10000)
	// @:Variant(100000)
	// @:variant(1000000)
	public function test_create(count:Int) {
		// sys.io.File.saveContent('./test/test.hxbk', '');
		Engine.start({path: 'test'});
		var book = Book.open('test');
		book.count().handle(initial -> {
			// book.addStoragePlan(
			// 	haxe.zip.Compress.run.bind(_, 1),
			// 	haxe.zip.Uncompress.run.bind(_, null)

			// );
			var eventStream = book.create([for (i in 0...count) new Record("DATA HERE")]);
			var expecting = 0;
			var log = [];
			var hits = 0;
			var sum = 0;
			var handler = eventStream.forEach(result -> {
				// trace('Result: $result');
				switch (result) {
					case Expect(amount):
						expecting = amount;
					case Created(page, records):
						hits++;
						log.push({
							page: page.number,
							count: records.length
						});
						sum += records.length;
				}
				return Resume;
			});
			handler.handle(result -> {
				
				book.commit().handle(() -> {
					trace(log);
					trace("DONE COMMITTING.");
					book.count().handle(total -> {
						var padding = book.calculatePadding();
						trace('total: $total, sum: $sum, initial: $initial, hits: $hits');
						asserts.assert(padding.percentage < 0.3);
						asserts.assert(result == Depleted);
						asserts.assert(count + initial == total);
						asserts.assert(total - sum == initial);
						// trace('LOG: $log');
						asserts.done();
					});
				});
			});
		});
		return asserts;
	}

	function request(count:Int):Promise<Dynamic> {
		var done = Future.trigger();
		var http = new haxe.Http('https://randomuser.me/api?results=$count');
		trace("Getting random users");
		http.onData = function(d) done.trigger(Success(haxe.Json.parse(d)));
		http.onError = function(e) done.trigger(Failure(e));
		http.request(false);
		return done.asFuture();
	}

	#if !eval
	// @:variant(25, 1)
	// @:variant(50, 2)
	// @:variant(100, 3)
	// @:variant(250, 4)
	// @:variant(500, 5)
	// @:variant(1000, 6)
	// @:variant(2000, 7)
	// @:variant(3000, 8)

	@:variant(1000, 9)
	public function test_http(count:Int, apiHits:Int) {
		Promise.inParallel([for (i in 0...apiHits) request(count)]).handle(results -> {
			// sys.io.File.saveContent('./http/randomuser.me.hxbk', '');
			switch (results) {
				case Success(randomUsers):
					Engine.start({path: 'http'});
					var book = Book.open('randomuser.me');

					book.count().handle(initialCount -> {
						var records = randomUsers.fold((val : Dynamic, aggregate : Array<Record>) -> {
							switch (val) {
								case Success(result):
									var userRecords = result.results.map(random -> new Record(random));
									aggregate = aggregate.concat(userRecords);
								default:
							}
							return aggregate;
						}, []);
						var eventStream = book.create(records);
						var expecting = 0;
						var log = [];
						var hits = 0;
						var handler = eventStream.forEach(result -> {
							// trace('Result: $result');
							switch (result) {
								case Expect(amount):
									expecting = amount;
								case Created(page, records):
									hits++;
									log.push({
										page: page.number,
										count: records.length
									});
							}
							return Resume;
						});
						handler.handle(result -> {
							book.commit().handle(() -> {
								book.count().handle(total -> {
									var padding = book.calculatePadding();

									asserts.assert(padding.percentage < 0.3);
									asserts.assert(result == Depleted);
									asserts.assert(initialCount + (count * apiHits) == total);
									trace(padding);
									// trace('LOG: $log');
									// trace('TOTAL: ${log.map(entry -> entry.count)}');
									asserts.done();
								});
							});
						});
					});
				default:
			}
			return Noise;
		});
		return asserts;
	}
	#end
}

@:exclude
@:asserts
class TestStream {
	public function new() {}

	public function test() {
		var done = false;
		var a = Signal.trigger();
		var stream = new SignalStream(a.asSignal());
		a.trigger(Data(1));
		a.trigger(Data(2));
		a.trigger(Data(3));
		var i = 0;
		var sum = 0;
		var result = stream.forEach(v -> {
			asserts.assert(++i == v);
			sum += v;
			return Resume;
		});

		a.trigger(Data(4));
		a.trigger(Data(5));
		a.trigger(End);
		a.trigger(Data(6));
		a.trigger(Data(7));

		result.handle(x -> {
			asserts.assert(Depleted == x);
			asserts.assert(15 == sum);
		});
		var nextResult = stream.forEach(v -> {
			sum += v;
			trace('sum: $sum, $v');
			return Resume;
		});
		a.trigger(End);
		nextResult.handle(x -> {
			asserts.assert(Depleted == x);
			asserts.assert(30 == sum);
			done = true;
		});
		asserts.assert(done);
		return asserts.done();
	}

	public function test_pass_stream() {
		var stream = get_stream();
		var sum = 0;
		var summation = stream.forEach(v -> {
			sum += v;
			return Resume;
		});
		summation.handle(handled -> {
			asserts.assert(sum == 15 && handled == Depleted);
			asserts.done();
		});
		return asserts;
	}

	function get_stream() {
		var a = Signal.trigger();
		var stream = new SignalStream(a.asSignal());
		a.trigger(Data(1));
		a.trigger(Data(2));
		a.trigger(Data(3));
		a.trigger(Data(4));
		a.trigger(Data(5));
		a.trigger(End);
		return stream;
	}

	public function test_unclog() {
		var a = Signal.trigger();
		var stream:RealStream<Int> = new SignalStream(a.asSignal());
		var aggregate = [];
		a.trigger(Data(1));
		a.trigger(Data(2));
		a.trigger(Data(-1));
		a.trigger(Data(3));
		a.trigger(Data(4));
		a.trigger(End);
		var f:Handler<Int, Error> = result -> {
			try {
				if (result == -1)
					throw new Error(InternalError, "Can't perform operation on -1");
				aggregate.push(result);
				return Resume;
			} catch (e:Error) {
				return Clog(e);
			}
		};
		var f2:Handler<Int, Error> = result -> {
			aggregate.push(result);
			return Resume;
		};
		var handler = stream.forEach(f);
		handler.handle(conclusion -> {
			switch (conclusion) {
				case Clogged(e, rest):
					trace('Threw $e, resuming');
					rest.forEach(f2).handle(conclusion -> {
						switch (conclusion) {
							case Depleted:
								trace('Done');
								var expected = [1, 2, -1, 3, 4];
								for (i in aggregate) {
									asserts.assert(i == expected[aggregate.indexOf(i)]);
								}
								asserts.done();
							default:
						}
						return Noise;
					});
				case Depleted:
					asserts.done();
				default:
			}
			return Noise;
		});
		return asserts;
	}
}
