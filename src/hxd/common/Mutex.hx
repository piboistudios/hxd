package hxd.common;

import tink.CoreApi;

class Mutex {
	public var locked(default, null):Bool;

	var queue:Array<FutureTrigger<Void->Void>>;

	public function new() {
		this.queue = [];
	}

	public function lock(urgent = false) {
		if (this.locked) {
			var trigger = Future.trigger();
			if (urgent) {
				this.queue.insert(0, trigger);
			} else {
				this.queue.push(trigger);
			}
			return trigger.asFuture();
		} else {
			this.locked = true;
            return this.unlock;
		}
	}

	public function unlock() {
		if (this.queue.length != 0) {
			// trace('Unlocking');/
			var next = this.queue.splice(0, 1)[0];
			next.trigger(this.unlock);
		} else {
			this.locked = false;
		}
	}
}