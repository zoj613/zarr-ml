open Bytes_to_bytes
open Array_to_array
open Array_to_bytes
open Util.Result_syntax

include Codecs_intf

type arraytobytes =
  [ `Bytes of endianness
  | `ShardingIndexed of shard_config ]

and shard_config =
  {chunk_shape : int array
  ;codecs :
    [ arraytoarray
    | `Bytes of endianness
    | `ShardingIndexed of shard_config
    | bytestobytes ] list
  ;index_codecs :
    [ arraytoarray
    | `Bytes of endianness
    | `ShardingIndexed of shard_config
    | fixed_bytestobytes ] list
  ;index_location : loc}

type codec_chain =
  [ arraytoarray | arraytobytes | bytestobytes ] list

module Chain = struct
  type t = bytestobytes internal_chain

  let rec create : 
    type a b. (a, b) Util.array_repr -> codec_chain -> (t, [> error ]) result
    = fun repr cc ->
    let a2a, rest =
      List.partition_map
        (function
        | #arraytoarray as c -> Either.left c
        | #arraytobytes as c -> Either.right c
        | #bytestobytes as c -> Either.right c) cc
    in
    List.fold_right
      (fun c acc ->
        acc >>= fun (l, r) ->
        match c with
        | #bytestobytes as c -> Ok (l, c :: r)
        | `Bytes e -> Ok (`Bytes e :: l, r)
        | `ShardingIndexed cfg ->
          create repr cfg.codecs >>= fun codecs ->
          create
            {repr with shape = Array.append repr.shape [|2|]}
            (cfg.index_codecs :> codec_chain) >>= fun index_codecs ->
          (* coerse to a fixed_bytestobytes internal_chain list type *)
          let b2b =
            fst @@
            List.partition_map
              (function
              | #fixed_bytestobytes as c -> Either.left c
              | c -> Either.right c) index_codecs.b2b
          in
          let cfg' : internal_shard_config =
            {codecs
            ;chunk_shape = cfg.chunk_shape
            ;index_location = cfg.index_location
            ;index_codecs = {index_codecs with b2b}}
          in Ok (`ShardingIndexed cfg' :: l, r)) rest (Ok ([], []))
    >>= fun result ->
    (match result with
    | [x], r -> Ok (x, r)
    | _ -> Error (`CodecChain "Must be exactly one array->bytes codec."))
    >>= fun (a2b, b2b) ->
    let ic = {a2a; a2b; b2b} in
    List.fold_left
      (fun acc c ->
         acc >>= fun r ->
         ArrayToArray.parse r c >>= fun () ->
         ArrayToArray.compute_encoded_representation c r) (Ok repr) ic.a2a
    >>= fun repr' ->
    ArrayToBytes.parse repr' ic.a2b >>| fun () -> ic

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
    | [] -> Ok {a2a; a2b; b2b}
    | x :: _ ->
      let msg =
        (Util.get_name x) ^
        " codec is unsupported or has invalid configuration." in
      Error msg
end
