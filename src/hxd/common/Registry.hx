package hxd.common;
import hxd.common.Mutex;

class Registry {
	static var accessLog:Map<String, Mutex> = new Map<String, Mutex>();
	public static function print() {
		trace(accessLog);
	}
	public static function acquire(resource:String) {
		if (!accessLog.exists(resource)) {
			accessLog.set(resource, new Mutex());
		}
        
		return accessLog[resource].lock();
	}

	public static function locked(resource:String) {
		if (!accessLog.exists(resource)) {
			return false;
		}
		return accessLog[resource].locked;
	}

	public static function release(resource:String) {
		if (!accessLog.exists(resource)) {
			return;
		}
		accessLog[resource].unlock();
	}
}