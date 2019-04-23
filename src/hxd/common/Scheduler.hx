package hxd.common;

import tink.concurrent.Thread;
import tink.runloop.Task;
import tink.runloop.Worker;
import tink.CoreApi;
import tink.RunLoop;
enum Status {
	Busy;
	Free;
}

class Scheduler {
	var worker:Worker;
	var status:Status;
    var current = 0;
    var next = 0;
	public function new() {
		worker = RunLoop.current.createSlave();
		status = Free;
	}

	public function schedule(task:Void->Void) {
		worker.work(() -> {
			if (status == Busy)
				return Continue;
			else {
				status = Busy;
				var thread = new Thread(() -> {
					task();
					status = Free;
				});
				return Done;
			}
		});
        return RunLoop.current.delegate(Noise, worker);
	}
    public function scheduleUntil(futureTask:Void->Future<Dynamic>) {
        var handle = current++;
        var done = Future.trigger();
        worker.work(() -> {
            trace('STATUS: $status');
            switch(status) {
                case Busy:
                    
                    return Continue;
                default:
                    trace('Proceeding with task $handle');
                status = Busy;
                var thread = new Thread(() -> {
                    try {

                    futureTask().handle(r -> {
                        trace("Freed: " + r);
                        status = Free;
                        next++;
                        done.trigger(Noise);
                    });
                    } catch(e:Dynamic) {
                        throw e;
                    }
                });
                do {

                } while(status == Busy);
                return Done;
            }
            
        });
        
        return done.asFuture();
    }

}
