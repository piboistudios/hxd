package hxd.storage;
import haxe.io.Bytes;

class Serializer {
	public static function serialize(object:Dynamic, ?transform:Bytes->Bytes) {
		transform = transform == null ? b->b : transform;
		return transform(Bytes.ofString(haxe.Serializer.run(object)));
	}

	public static function deserialize(bytes:Bytes, ?transform:Bytes->Bytes) {
		transform = transform == null ? b->b : transform;
		return haxe.Unserializer.run(transform(bytes).toString());
	}
}