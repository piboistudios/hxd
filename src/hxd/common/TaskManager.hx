package hxd.common;
import tink.CoreApi;
class TaskManager {
    var schedulers:Array<Scheduler>;
    var index:Int;
    public function new(count = 4) {
        schedulers = [for(i in 0...4) new Scheduler()];
        index = 0;
    }
    public function schedule(task:Void->Void) {
        var nextScheduler = schedulers[index];
        index = (index + 1) % schedulers.length;
        return nextScheduler.schedule(task);
    }
    public function scheduleUntil(futureTask:Void->Future<Noise>) {
        var nextScheduler = schedulers[index];
        index = (index + 1) % schedulers.length;
        return nextScheduler.scheduleUntil(futureTask);
    }
}