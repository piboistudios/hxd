package hxd.sys;

import sys.FileSystem;
import haxe.DynamicAccess;
import tink.core.Error.ErrorCode;
import tink.CoreApi.Error;
import hxd.sys.Messages;

class Engine {
	static var instance:Engine;

	var cfg:Dynamic = {
		path: '',
		book: {
			pageSize: 16000,
			maxInsertSize: 250
		}
	};

	public static var config(get, never):Dynamic;

	public static function get_config() {
		return instance.cfg;
	}

	public static var path(get, never):String;

	public static function get_path() {
		return instance.cfg.path;
	}

	public function new() {}

	public function setConfig(c) {
		cfg = c;
	}

	public static function start(?config:haxe.DynamicAccess<Any>) {
		Engine.instance = new Engine();
		if (config == null)
			return;

		var dAccessConfig:DynamicAccess<Dynamic> = @:privateAccess instance.cfg;

		for (key in config.keys()) {
			trace(key);
			trace(dAccessConfig);
			trace(config);
			dAccessConfig.set(key, config.get(key));
			trace(dAccessConfig);
		}
	}
	
	public static function ensure() {
		if (Engine.instance == null)
			throw new Error(InternalError, NotInitialized);
		if (!FileSystem.exists('./${Engine.path}')) {
			FileSystem.createDirectory('./${Engine.path}');
		}
	}
}
