package hxd.storage;
import hxd.storage.*;
import haxe.ds.BalancedTree;
using Lambda;
import tink.CoreApi;
import haxe.io.Bytes;
using hxd.storage.Serializer;
class Page {
	var book:Book;
	
	public var number(default, null):Ref<Int>;
	public var dirty(default, null):Bool = false;
	public var records(default, null):haxe.ds.BalancedTree<Int, Record>;

	public function new(b:Book) {
		book = b;
		number = -1;
		records = new haxe.ds.BalancedTree<Int, Record>();
	}

	var _bytes:Bytes;

	public var bytes(get, never):Bytes;

	public function get_bytes() {
		// if (_bytes == null)
			_bytes = this.serialize(book.postSerialization);
		return _bytes;
	}

	var _size:Null<Int>;

	public var size(get, never):Int;

	public function get_size() {
		// if (_size == null)
			_size = bytes.length;
		return _size;
	}

	public function touch() {
		dirty = true;
		_size = null;
		_bytes = null;
	}

	function cleanse() {
		dirty = false;
		_size = null;
		_bytes = null;
	}

	function attempt(operation:Void->Void) {
		var tmpRecords = records.array();
		var wasDirty = dirty;
		try {
			touch();
			operation();
		} catch (e:Dynamic) {
			throw e;
		}
		if (size + 128 >= book.pageSize) {
			records = new BalancedTree<Int, Record>();
			tmpRecords.iter(record -> records.set(record.index, record));
			if (!wasDirty)
				cleanse();
			return false;
		}

		return true;
	}

	public function create(r:Array<Record>) {
		return attempt(() -> {
			for (record in r) {
				var lastRecord = records.get(records.count() - 1);
				var nextIndex = lastRecord != null ? lastRecord.index + 1 : 0;
				@:privateAccess record.index = nextIndex;
				records.set(nextIndex, record);
			}
		});
	}

	public function retrieve(predicate:Record->Bool) {
		return records.filter(predicate);
	}

	public function update(predicate:Record->Bool, transformation:Record->Void) {
		return attempt(() -> {
			records.filter(predicate).iter(transformation);
		});
	}

	public function delete(predicate:Record->Bool) {
		return attempt(() -> {
			var reversePredicate = d -> !predicate(d);
			records.filter(reversePredicate).iter(record -> records.remove(record.index));
		});
	}

	@:keep
	public function hxSerialize(s:haxe.Serializer) {
		s.serialize(records.array());
	}

	@:keep
	public function hxUnserialize(u:haxe.Unserializer) {
		var r:Array<Record> = u.unserialize();
		records = new BalancedTree<Int, Record>();
		for(record in r) {
			records.set(record.index, record);
		}
	}
}

