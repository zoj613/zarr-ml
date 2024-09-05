open Codecs_intf

module ArrayToBytes : sig
  val parse : arraytobytes -> int array -> unit
  val encoded_size : int -> fixed_arraytobytes -> int
  val encode : arraytobytes -> 'a Ndarray.t -> string
  val decode : arraytobytes -> 'a array_repr -> string -> 'a Ndarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (arraytobytes, string) result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end

module Make (Io : Types.IO) : sig
  open Io
  type t = internal_shard_config

  val partial_encode :
    t ->
    ((int * int option) list -> string list Deferred.t) ->
    (?append:bool -> (int * string) list -> unit Deferred.t) ->
    int ->
    'a array_repr ->
    (int array * 'a) list ->
    unit Deferred.t
  val partial_decode :
    t ->
    ((int * int option) list -> string list Io.Deferred.t) ->
    int ->
    'a array_repr ->
    (int * int array) list ->
    (int * 'a) list Deferred.t
end
