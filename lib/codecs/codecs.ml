include Bytes_to_bytes
include Array_to_array
include Array_to_bytes
open Util.Result_syntax

module Ndarray = Owl.Dense.Ndarray.Generic

type arraytoarray =
  [ `Transpose of int array ]

type fixed_bytestobytes =
  [ `Crc32c ]

type variable_bytestobytes =
  [ `Gzip of compression_level ]

type bytestobytes =
  [ fixed_bytestobytes | variable_bytestobytes ]

type arraytobytes =
  [ `Bytes of endianness
  | `ShardingIndexed of shard_config ]

and shard_config =
  {chunk_shape : int array
  ;codecs : bytestobytes shard_chain
  ;index_codecs : fixed_bytestobytes shard_chain
  ;index_location : loc}

and 'a shard_chain = {
  a2a: arraytoarray list;
  a2b: arraytobytes;
  b2b: 'a list;
}

type codec_chain = {
  a2a: arraytoarray list;
  a2b: arraytobytes;
  b2b: bytestobytes list;
}

let rec to_internal_a2b v =
  match v with
  | `Bytes e -> Bytes e
  | `ShardingIndexed cfg ->
    ShardingIndexed
      {chunk_shape = cfg.chunk_shape
      ;index_location = cfg.index_location
      ;index_codecs : fixed bytes_to_bytes sharding_chain = 
        {a2a = to_internal_a2a cfg.index_codecs.a2a
        ;a2b = to_internal_a2b cfg.index_codecs.a2b
        ;b2b = fixed_to_internal_b2b cfg.index_codecs.b2b}
      ;codecs : any_bytes_to_bytes sharding_chain =
        {a2a = to_internal_a2a cfg.codecs.a2a
        ;a2b = to_internal_a2b cfg.codecs.a2b
        ;b2b = variable_to_internal_b2b cfg.codecs.b2b}}

and to_internal_a2a a2a =
  List.fold_right
    (fun x acc ->
      match x with
      | `Transpose o -> Transpose o :: acc) a2a []

and fixed_to_internal_b2b b2b =
  List.fold_right
    (fun x acc ->
      match x with
      | `Crc32c -> Crc32c :: acc) b2b []

and variable_to_internal_b2b b2b =
  List.fold_right
    (fun x acc ->
      match x with
      | `Gzip lvl -> Any (Gzip lvl) :: acc
      | `Crc32c -> Any Crc32c :: acc) b2b []

module Chain = struct
  type t = any_bytes_to_bytes sharding_chain

  let create : 
    type a b. (a, b) Util.array_repr -> codec_chain -> (t, [> error ]) result
    = fun repr cc ->
    let a2a = to_internal_a2a cc.a2a in
    let a2b = to_internal_a2b cc.a2b in
    let b2b = variable_to_internal_b2b cc.b2b in
    List.fold_left
      (fun acc c ->
         acc >>= fun r ->
         ArrayToArray.parse r c >>= fun () ->
         ArrayToArray.compute_encoded_representation c r) (Ok repr) a2a
    >>= fun repr' ->
    ArrayToBytes.parse repr' a2b >>| fun () ->
    ({a2a; a2b; b2b} : t)

  let default : t =
    {a2a = []; a2b = ArrayToBytes.default; b2b = []}

  let encode :
    type a b. t -> (a, b) Ndarray.t -> (string, [> error ]) result
    = fun t x ->
    List.fold_left
      (fun acc c -> acc >>= ArrayToArray.encode c) (Ok x) t.a2a
    >>= fun y ->
    List.fold_left
      (fun acc c -> acc >>= BytesToBytes.encode c)
      (ArrayToBytes.encode y t.a2b) t.b2b

  let decode :
    type a b.
    t ->
    (a, b) Util.array_repr ->
    string ->
    ((a, b) Ndarray.t, [> error ]) result
    = fun t repr x ->
    (* compute the last encoded representation of array->array codec chain.
       This becomes the decoded representation of the array->bytes decode
       procedure. *)
    List.fold_left
      (fun acc c ->
        acc >>= ArrayToArray.compute_encoded_representation c)
      (Ok repr) t.a2a
    >>= fun repr' ->
    List.fold_right
      (fun c acc -> acc >>= BytesToBytes.decode c) t.b2b (Ok x)
    >>= fun y ->
    List.fold_right
      (fun c acc -> acc >>= ArrayToArray.decode c)
      t.a2a (ArrayToBytes.decode y repr' t.a2b)

  let ( = ) : t -> t -> bool = fun x y ->
    x.a2a = y.a2a && x.a2b = y.a2b && x.b2b = y.b2b

  let to_yojson : t -> Yojson.Safe.t = fun t ->
    `List
      (List.map ArrayToArray.to_yojson t.a2a @
      (ArrayToBytes.to_yojson t.a2b) ::
      List.map BytesToBytes.to_yojson t.b2b)

  let of_yojson : Yojson.Safe.t -> (t, string) result = fun x ->
    let filter_partition f encoded =
      List.fold_right (fun c (l, r) ->
        match f c with
        | Ok v -> v :: l, r
        | Error _ -> l, c :: r) encoded ([], [])
    in
    (match Yojson.Safe.Util.to_list x with
    | [] -> Error "No codec specified."
    | y -> Ok y)
    >>= fun codecs ->
    (match filter_partition ArrayToBytes.of_yojson codecs with
    | [x], rest -> Ok (x, rest)
    | _ -> Error "Must be exactly one array->bytes codec.")
    >>= fun (a2b, rest) ->
    let a2a, rest = filter_partition ArrayToArray.of_yojson rest in
    let b2b, rest = filter_partition BytesToBytes.of_yojson rest in
    match rest with
    | [] -> Ok ({a2a; a2b; b2b} : t)
    | x :: _ ->
      let msg =
        (Util.get_name x) ^
        " codec is unsupported or has invalid configuration." in
      Error msg
end
