open Codecs_intf

module Ndarray = Owl.Dense.Ndarray.Generic

module ArrayToBytes : sig
  val parse
    : ('a, 'b) Util.array_repr ->
      arraytobytes ->
      (unit, [> error]) result
  val compute_encoded_size : int -> arraytobytes -> int
  val default : arraytobytes
  val encode
    : ('a, 'b) Ndarray.t ->
      arraytobytes ->
      (string, [> error]) result
  val decode
    : string ->
      ('a, 'b) Util.array_repr ->
      arraytobytes ->
      (('a, 'b) Ndarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (arraytobytes, string) result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end
