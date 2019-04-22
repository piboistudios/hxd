package hxd.sys;
@:enum
abstract Messages(String) from String to String {
    var NotInitialized = "Engine must be initialized before beginning operation.";
}
