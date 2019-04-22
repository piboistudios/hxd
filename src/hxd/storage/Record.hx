    
package hxd.storage;


class Record {
	public var index(default, null):Int;
	public var data(default, null):Dynamic;

	public function new(d) {
        index = -1;
		data = d;
	}
    @:keep
    public function hxSerialize(s : haxe.Serializer) {
        s.serialize(index);
        s.serialize(data);
    }
    @:keep
    public function hxUnserialize(u : haxe.Unserializer) {
        try {

        index = u.unserialize();
        data = u.unserialize();
        
        
        } catch(e:Dynamic) {
            
            throw e;
        }
    }
}