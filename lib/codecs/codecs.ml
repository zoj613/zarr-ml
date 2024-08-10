open Bytes_to_bytes
open Array_to_array
open Array_to_bytes
open Util.Result_syntax

include Codecs_intf

type fixed_arraytobytes =
  [ `Bytes of endianness ]

type variable_array_tobytes =
  [ `ShardingIndexed of shard_config ]

and shard_config =
  {chunk_shape : int array
  ;codecs :
    [ arraytoarray
    | fixed_arraytobytes
    | `ShardingIndexed of shard_config
    | bytestobytes ] list
  ;index_codecs :
    [ arraytoarray | fixed_arraytobytes | fixed_bytestobytes ] list
  ;index_location : loc}

type array_tobytes =
  [ fixed_arraytobytes | variable_array_tobytes ]

type codec_chain =
  [ arraytoarray | array_tobytes | bytestobytes ] list

module Chain = struct
  type t = (arraytobytes, bytestobytes) internal_chain

  let rec create : 
    type a b. int array -> codec_chain -> (t, [> error ]) result
    = fun shape cc ->
    let a2a, rest =
      List.partition_map
        (function
        | #arraytoarray as c -> Either.left c
        | #array_tobytes as c -> Either.right c
        | #bytestobytes as c -> Either.right c) cc
    in
    List.fold_right
      (fun c acc ->
        acc >>= fun (l, r) ->
        match c with
        | #bytestobytes as c -> Ok (l, c :: r)
        | #fixed_arraytobytes as c -> Ok (c :: l, r)
        | `ShardingIndexed cfg ->
          create shape cfg.codecs >>= fun codecs ->
          create
            (Array.append shape [|2|])
            (cfg.index_codecs :> codec_chain) >>= fun index_codecs ->
          (* coerse to a fixed codec internal_chain list type *)
          let b2b =
            fst @@
            List.partition_map
              (function
              | #fixed_bytestobytes as c -> Either.left c
              | c -> Either.right c) index_codecs.b2b
          in
          let a2b =
            List.hd @@
            fst @@
            List.partition_map
              (function
              | #fixed_arraytobytes as c -> Either.left c
              | c -> Either.right c) [index_codecs.a2b]
          in
          let cfg' : internal_shard_config =
            {codecs
            ;chunk_shape = cfg.chunk_shape
            ;index_location = cfg.index_location
            ;index_codecs = {index_codecs with a2b; b2b}}
          in Ok (`ShardingIndexed cfg' :: l, r)) rest (Ok ([], []))
    >>= fun result ->
    (match result with
    | [x], r -> Ok (x, r)
    | _ -> Error (`CodecChain "Must be exactly one array->bytes codec."))
    >>= fun (a2b, b2b) ->
    ArrayToBytes.parse a2b @@
    (match a2a with
    | [] -> shape
    | l -> 
      ArrayToArray.parse (List.hd l) shape;
      List.fold_left ArrayToArray.compute_encoded_representation shape l)
    >>| fun () -> {a2a; a2b; b2b}

  let encode :
    t ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    (string, [> error ]) result
    = fun t x ->
    ArrayToBytes.encode t.a2b @@ List.fold_left ArrayToArray.encode x t.a2a
    >>| fun y -> List.fold_left BytesToBytes.encode y t.b2b

  let is_just_sharding : t -> bool = function
    | {a2a = []; a2b = `ShardingIndexed _; b2b = []} -> true
    | _ -> false

  let partial_encode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error] as 'c) result) ->
    partial_setter ->
    int ->
    ('a, 'b) array_repr ->
    (int array * 'a) list ->
    (unit, 'c) result
    = fun t f g bsize repr pairs ->
    match t.a2b with
    | `ShardingIndexed c ->
      ShardingIndexedCodec.partial_encode c f g bsize repr pairs
    | `Bytes _ -> failwith "bytes codec does not support partial encoding." 

  let partial_decode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    int ->
    ('a, 'b) array_repr ->
    (int * int array) list ->
    ((int * 'a) list, 'c) result
    = fun t f s repr pairs ->
    match t.a2b with
    | `ShardingIndexed c ->
      ShardingIndexedCodec.partial_decode c f s repr pairs
    | `Bytes _ -> failwith "bytes codec does not support partial decoding."

  let decode :
    t ->
    ('a, 'b) array_repr ->
    string ->
    (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
    ,[> `Store_read of string | error]) result
    = fun t repr x ->
    let shape =
      List.fold_left
        ArrayToArray.compute_encoded_representation repr.shape t.a2a in
    let y = List.fold_right BytesToBytes.decode t.b2b x in
    ArrayToBytes.decode t.a2b {repr with shape} y >>| fun a ->
    List.fold_right ArrayToArray.decode t.a2a a

  let ( = ) : t -> t -> bool = fun x y ->
    x.a2a = y.a2a && x.a2b = y.a2b && x.b2b = y.b2b

  let to_yojson : t -> Yojson.Safe.t = fun t ->
    `List
      (List.map ArrayToArray.to_yojson t.a2a @
      (ArrayToBytes.to_yojson t.a2b) ::
      List.map BytesToBytes.to_yojson t.b2b)

  let of_yojson :
    int array -> Yojson.Safe.t -> (t, string) result
    = fun chunk_shape x ->
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
    (match filter_partition (ArrayToBytes.of_yojson chunk_shape) codecs with
    | [x], rest -> Ok (x, rest)
    | _ -> Error "Must be exactly one array->bytes codec.")
    >>= fun (a2b, rest) ->
    let a2a, rest = filter_partition (ArrayToArray.of_yojson chunk_shape) rest in
    let b2b, rest = filter_partition BytesToBytes.of_yojson rest in
    match rest with
    | [] -> Ok {a2a; a2b; b2b}
    | x :: _ ->
      let msg =
        (Util.get_name x) ^
        " codec is unsupported or has invalid configuration." in
      Error msg
end
