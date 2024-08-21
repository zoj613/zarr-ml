open Array_to_array
open Bytes_to_bytes
open Codecs_intf

module Ndarray = Owl.Dense.Ndarray.Generic
module Indexing = Util.Indexing
module ArrayMap = Util.ArrayMap
module RegularGrid = Extensions.RegularGrid

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/bytes/v1.0.html *)
module BytesCodec = struct
  let encoded_size (input_size : int) = input_size

  let endian_module = function
    | LE -> (module Ebuffer.Little : Ebuffer.S)
    | BE -> (module Ebuffer.Big : Ebuffer.S)

  let encode : type a b. (a, b) Ndarray.t -> endianness -> string = fun x t ->
    let open (val endian_module t) in
    let buf = Buffer.create @@ Ndarray.size_in_bytes x in
    match Ndarray.kind x with
    | Char -> Ndarray.iter (add_char buf) x; contents buf
    | Int8_signed -> Ndarray.iter (add_int8 buf) x; contents buf
    | Int8_unsigned -> Ndarray.iter (add_uint8 buf) x; contents buf
    | Int16_signed -> Ndarray.iter (add_int16 buf) x; contents buf
    | Int16_unsigned -> Ndarray.iter (add_uint16 buf) x; contents buf
    | Int32 -> Ndarray.iter (add_int32 buf) x; contents buf
    | Int64 -> Ndarray.iter (add_int64 buf) x; contents buf
    | Float32 -> Ndarray.iter (add_float32 buf) x; contents buf
    | Float64 -> Ndarray.iter (add_float64 buf) x; contents buf
    | Complex32 -> Ndarray.iter (add_complex32 buf) x; contents buf
    | Complex64 -> Ndarray.iter (add_complex64 buf) x; contents buf
    | Int -> Ndarray.iter (add_int buf) x; contents buf
    | Nativeint -> Ndarray.iter (add_nativeint buf) x; contents buf

  let decode :
    type a b. string -> (a, b) array_repr -> endianness -> (a, b) Ndarray.t
    = fun buf decoded t ->
    let open Bigarray in
    let open (val endian_module t) in
    let k, shp = decoded.kind, decoded.shape in
    match k, kind_size_in_bytes k with
    | Char, _ -> Ndarray.init k shp @@ get_char buf
    | Int8_signed, _ -> Ndarray.init k shp @@ get_int8 buf
    | Int8_unsigned, _ -> Ndarray.init k shp @@ get_uint8 buf
    | Int16_signed, s -> Ndarray.init k shp @@ fun i -> get_int16 buf (i*s)
    | Int16_unsigned, s -> Ndarray.init k shp @@ fun i -> get_uint16 buf (i*s)
    | Int32, s -> Ndarray.init k shp @@ fun i -> get_int32 buf (i*s)
    | Int64, s -> Ndarray.init k shp @@ fun i -> get_int64 buf (i*s)
    | Float32, s -> Ndarray.init k shp @@ fun i -> get_float32 buf (i*s)
    | Float64, s -> Ndarray.init k shp @@ fun i -> get_float64 buf (i*s)
    | Complex32, s -> Ndarray.init k shp @@ fun i -> get_complex32 buf (i*s)
    | Complex64, s -> Ndarray.init k shp @@ fun i -> get_complex64 buf (i*s)
    | Int, s -> Ndarray.init k shp @@ fun i -> get_int buf (i*s)
    | Nativeint, s -> Ndarray.init k shp @@ fun i -> get_nativeint buf (i*s)

  let to_yojson e =
    let endian =
      match e with
      | LE -> "little"
      | BE -> "big"
    in
    `Assoc
    [("name", `String "bytes")
    ;("configuration", `Assoc ["endian", `String endian])]

  let of_yojson x =
    match Yojson.Safe.Util.(member "configuration" x |> to_assoc) with
    | [("endian", `String e)] ->
      (match e with
      | "little" -> Ok LE
      | "big" -> Ok BE
      | s -> Error (Printf.sprintf "Unsupported endianness: %s" s))
    | _ -> Error "Invalid bytes codec configuration."
end

module Any = Owl.Dense.Ndarray.Any

module rec ArrayToBytes : sig
  val parse : arraytobytes -> int array -> unit
  val encoded_size : int -> fixed_arraytobytes -> int
  val encode : arraytobytes -> ('a, 'b) Ndarray.t -> string
  val decode : arraytobytes -> ('a, 'b) array_repr -> string -> ('a, 'b) Ndarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (arraytobytes, string) result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end = struct

  let parse t shp =
    match t with
    | `Bytes _ -> ()
    | `ShardingIndexed c -> ShardingIndexedCodec.parse c shp

  let encoded_size :
    int -> fixed_arraytobytes -> int
    = fun input_size -> function
    | `Bytes _ -> BytesCodec.encoded_size input_size

  let encode t x =
    match t with
    | `Bytes endian -> BytesCodec.encode x endian
    | `ShardingIndexed c -> ShardingIndexedCodec.encode c x

  let decode t repr b =
    match t with
    | `Bytes endian -> BytesCodec.decode b repr endian
    | `ShardingIndexed c -> ShardingIndexedCodec.decode c repr b

  let to_yojson = function
    | `Bytes endian -> BytesCodec.to_yojson endian
    | `ShardingIndexed c -> ShardingIndexedCodec.to_yojson c

  let of_yojson shp x =
    match Util.get_name x with
    | "bytes" -> Result.map (fun e -> `Bytes e) @@ BytesCodec.of_yojson x
    | "sharding_indexed" ->
      Result.map
        (fun c -> `ShardingIndexed c) @@ ShardingIndexedCodec.of_yojson shp x
    | _ -> Error ("array->bytes codec not supported: ")
end

and ShardingIndexedCodec : sig
  type t = internal_shard_config
  val parse : t -> int array -> unit
  val encode : t -> ('a, 'b) Ndarray.t -> string
  val decode : t -> ('a, 'b) array_repr -> string -> ('a, 'b) Ndarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
  val encode_chain :
    (arraytobytes, bytestobytes) internal_chain ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    string
  val decode_chain :
    (arraytobytes, bytestobytes) internal_chain ->
    ('a, 'b) array_repr ->
    string ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
  val decode_index :
    t -> int array -> string -> Stdint.uint64 Any.arr * string
  val index_size : 
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain -> int array -> int
  val encode_index_chain :
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain ->
    Stdint.uint64 Any.arr ->
    string
end = struct
  type t = internal_shard_config  

  let parse_chain :
    int array -> (arraytobytes, bytestobytes) internal_chain -> unit
    = fun shape chain ->
    let shape' =
      match chain.a2a with
      | [] -> shape
      | x :: _ as xs ->
        ArrayToArray.parse x shape;
        List.fold_left ArrayToArray.encoded_repr shape xs
    in ArrayToBytes.parse chain.a2b shape'

  let parse t shape =
    if Array.(length shape <> length t.chunk_shape)
    || not @@ Array.for_all2 (fun x y -> (x mod y) = 0) shape t.chunk_shape
    then raise Invalid_sharding_chunk_shape
    else
      parse_chain shape t.codecs;
      parse_chain
        (Array.append shape [|2|])
        (t.index_codecs :> (arraytobytes, bytestobytes) internal_chain)

  let encoded_size init chain =
    List.fold_left
      BytesToBytes.encoded_size
        (ArrayToBytes.encoded_size
           (List.fold_left
              ArrayToArray.encoded_size init chain.a2a) chain.a2b) chain.b2b
  
  let encode_chain chain x =
    let y =
      ArrayToBytes.encode chain.a2b @@
      List.fold_left ArrayToArray.encode x chain.a2a in
    List.fold_left BytesToBytes.encode y chain.b2b

  let encode_index_chain :
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain ->
    Stdint.uint64 Any.arr ->
    string
    = fun t x ->
    let y =
      match t.a2a with
      | [] -> x
      | `Transpose o :: _ -> Any.transpose ~axis:o x in
    let buf = Bytes.create (8 * Any.numel y) in
    (match t.a2b with
    | `Bytes LE ->
      Any.iteri (fun i v -> Stdint.Uint64.to_bytes_little_endian v buf (i*8)) y
    | `Bytes BE ->
      Any.iteri (fun i v -> Stdint.Uint64.to_bytes_big_endian v buf (i*8)) y);
    List.fold_left
      BytesToBytes.encode (Bytes.to_string buf) (t.b2b :> bytestobytes list)

  let encode t x =
    let shard_shape = Ndarray.shape x in
    let cps = Array.map2 (/) shard_shape t.chunk_shape in
    let idx_shp = Array.append cps [|2|] in
    let shard_idx = Any.create idx_shp Stdint.Uint64.max_int in
    let grid = RegularGrid.create ~array_shape:shard_shape t.chunk_shape in
    let slice = Array.make (Ndarray.num_dims x) @@ Owl_types.R [] in
    let kind = Ndarray.kind x in
    let m =
      Array.fold_right
        (fun y acc ->
          let k, c = RegularGrid.index_coord_pair grid y in
          ArrayMap.add_to_list k (c, Ndarray.get x y) acc)
        (Indexing.coords_of_slice slice shard_shape) ArrayMap.empty
    in
    let xs =
      snd @@
      ArrayMap.fold
        (fun idx pairs (ofs, xs) ->
          let v = Array.of_list @@ snd @@ List.split pairs in
          let x' = Ndarray.of_array kind v t.chunk_shape in
          let b = encode_chain t.codecs x' in
          let nb = Stdint.Uint64.of_int @@ String.length b in
          Any.set shard_idx (Array.append idx [|0|]) ofs;
          Any.set shard_idx (Array.append idx [|1|]) nb;
          Stdint.Uint64.(ofs + nb), b :: xs) m @@ (Stdint.Uint64.zero, [])
    in
    let ib = encode_index_chain t.index_codecs shard_idx in
    match t.index_location with
    | Start -> String.concat "" @@ ib :: List.rev xs
    | End -> String.concat "" @@ List.rev @@ ib :: xs

  let decode_chain t repr x =
    let shape = List.fold_left ArrayToArray.encoded_repr repr.shape t.a2a in
    List.fold_right ArrayToArray.decode t.a2a @@
    ArrayToBytes.decode t.a2b {repr with shape} @@
    List.fold_right BytesToBytes.decode t.b2b x

  let decode_index_chain :
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain ->
    int array ->
    string ->
    Stdint.uint64 Any.arr
    = fun t shape x ->
    let shape' = List.fold_left ArrayToArray.encoded_repr shape t.a2a in
    let y = List.fold_right BytesToBytes.decode (t.b2b :> bytestobytes list) x in
    let buf = Bytes.of_string y in
    let arr =
      match t.a2b with
      | `Bytes LE ->
        Any.init shape' @@ fun i ->
          Stdint.Uint64.of_bytes_little_endian buf (i*8)
      | `Bytes BE ->
        Any.init shape' @@ fun i ->
          Stdint.Uint64.of_bytes_big_endian buf (i*8)
    in
    match t.a2a with
    | [] -> arr
    | `Transpose o :: _ ->
      let inv_order = Array.(make (length o) 0) in
      Array.iteri (fun i x -> inv_order.(x) <- i) o;
      Any.transpose ~axis:inv_order arr

  let index_size index_chain cps =
    encoded_size (16 * Util.prod cps) index_chain

  let decode_index t cps b =
    let l = index_size t.index_codecs cps in
    let o = String.length b - l in
    let ib, rest =
      match t.index_location with
      | End -> String.sub b o l, String.sub b 0 o
      | Start -> String.sub b 0 l, String.sub b l o in
    decode_index_chain t.index_codecs (Array.append cps [|2|]) ib, rest

  let decode t repr b =
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let idx_arr, b' = decode_index t cps b in
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let slice = Array.make (Array.length repr.shape) @@ Owl_types.R [] in
    let icoords =
      Array.mapi
        (fun i v -> i, v) @@ Indexing.coords_of_slice slice repr.shape in
    let m =
      Array.fold_left
        (fun acc (i, y) ->
          let k, c = RegularGrid.index_coord_pair grid y in
          ArrayMap.add_to_list k (i, c) acc) ArrayMap.empty icoords in
    let inner = {repr with shape = t.chunk_shape} in
    let v =
      Array.of_list @@ snd @@ List.split @@
      List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) @@
      List.concat_map
        (fun (idx, pairs) ->
          let oc = Array.append idx [|0|] in
          let nc = Array.append idx [|1|] in
          let ofs = Stdint.Uint64.to_int @@ Any.get idx_arr oc in
          let nb = Stdint.Uint64.to_int @@ Any.get idx_arr nc in
          let arr = decode_chain t.codecs inner @@ String.sub b' ofs nb in
          List.map (fun (i, c) -> i, Ndarray.get arr c) pairs)
        (ArrayMap.bindings m)
    in Ndarray.of_array inner.kind v repr.shape

  let chain_to_yojson chain =
    `List
      (List.map ArrayToArray.to_yojson chain.a2a @
      (ArrayToBytes.to_yojson chain.a2b) ::
      List.map BytesToBytes.to_yojson chain.b2b)

  let to_yojson t =
    let index_codecs =
      chain_to_yojson
        (t.index_codecs :> (arraytobytes, bytestobytes) internal_chain) in
    let index_location =
      match t.index_location with
      | End -> `String "end"
      | Start -> `String "start"
    in
    let chunk_shape = 
      `List (
        Array.to_list @@
        Array.map (fun x -> `Int x) t.chunk_shape)
    in
    `Assoc
    [("name", `String "sharding_indexed");
     ("configuration",
      `Assoc
      [("chunk_shape", chunk_shape);
       ("index_location", index_location);
       ("index_codecs", index_codecs);
       ("codecs", chain_to_yojson t.codecs)])]

  let chain_of_yojson chunk_shape codecs =
    let open Util.Result_syntax in
    let filter_partition f encoded =
      List.fold_right (fun c (l, r) ->
        match f c with
        | Ok v -> v :: l, r
        | Error _ -> l, c :: r) encoded ([], [])
    in
    (match codecs with
    | [] -> Error "No codec chain specified for sharding_indexed."
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
      Result.error @@
      Printf.sprintf
        "%s codec is unsupported or has invalid configuration." @@ Util.get_name x

  let of_yojson shard_shape x =
    let open Util.Result_syntax in
    let assoc = Yojson.Safe.Util.(member "configuration" x |> to_assoc) in
    let extract name =
      Yojson.Safe.Util.filter_map
        (fun (n, v) -> if n = name then Some v else None) assoc in
    (match extract "chunk_shape" with
    | [] -> Error ("sharding_indexed must contain a chunk_shape field")
    | x :: _ ->
      List.fold_right (fun a acc ->
        acc >>= fun k ->
        match a with
        | `Int i when i > 0 -> Ok (i :: k)
        | _ -> Error "chunk_shape must only contain positive integers.")
        (Yojson.Safe.Util.to_list x) (Ok []))
    >>= fun l'->
    let chunk_shape = Array.of_list l' in
    (match extract "index_location" with
    | [] -> Error "sharding_indexed must have a index_location field"
    | x :: _ ->
      match x with
      | `String "end" -> Ok End
      | `String "start" -> Ok Start
      | _ -> Error "index_location must only be 'end' or 'start'")
    >>= fun index_location ->
    (match extract "codecs" with
    | [] -> Error "sharding_indexed must have a codecs field"
    | x :: _ ->
      chain_of_yojson chunk_shape @@ Yojson.Safe.Util.to_list x)
    >>= fun codecs ->
    (match extract "index_codecs" with
    | [] -> Error "sharding_indexed must have a index_codecs field"
    | x :: _ ->
      let cps = Array.map2 (/) shard_shape chunk_shape in
      let idx_shape = Array.append cps [|2|] in
      chain_of_yojson idx_shape @@ Yojson.Safe.Util.to_list x)
    >>= fun ic ->
    (* Ensure index_codecs only contains fixed size
       array->bytes and bytes->bytes codecs. *)
    let msg = "index_codecs must not contain variable-sized codecs." in
    List.fold_right
      (fun c acc ->
        acc >>= fun l ->
        match c with
        | `Crc32c -> Ok (`Crc32c :: l)
        | `Gzip _ -> Error msg) ic.b2b (Ok [])
    >>= fun b2b ->
    (match ic.a2b with
      | `Bytes e -> Ok (`Bytes e)
      | `ShardingIndexed _ -> Error msg)
    >>| fun a2b ->
    {index_codecs = {ic with a2b; b2b}; index_location; codecs; chunk_shape}
end

module Make (Io : Types.IO) = struct
  open Io
  open Deferred.Infix
  open ShardingIndexedCodec
  type t = ShardingIndexedCodec.t

  type set_fn = ?append:bool -> (int * string) list -> unit Deferred.t

  let partial_encode t get_partial (set_partial : set_fn) csize repr pairs =
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let is = index_size t.index_codecs cps in
    let l, pad =
      match t.index_location with
      | Start -> get_partial [0, Some is], is
      | End -> get_partial [csize - is, None], 0 in
    l >>= fun xs ->
    let idx_arr = fst @@ decode_index t cps @@ List.hd xs in
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let m =
      List.fold_left
        (fun acc (c, v) ->
          let id, co = RegularGrid.index_coord_pair grid c in
          ArrayMap.add_to_list id (co, v) acc) ArrayMap.empty pairs
    in
    let inner = {repr with shape = t.chunk_shape} in
    Deferred.fold_left
      (fun acc (idx, z) ->
        let oc = Array.append idx [|0|] in
        let nc = Array.append idx [|1|] in
        let ofs = Stdint.Uint64.to_int @@ Any.get idx_arr oc in
        let nb = Stdint.Uint64.to_int @@ Any.get idx_arr nc in
        get_partial [pad + ofs, Some nb] >>= fun l ->
        let arr = decode_chain t.codecs inner @@ List.hd l in
        List.iter (fun (c, v) -> Ndarray.set arr c v) z;
        let s = encode_chain t.codecs arr in
        let nb' = String.length s in
        (* if codec chain doesnt contain compression, update chunk in-place *)
        if nb' = nb then set_partial [pad + ofs, s] >>| fun () -> acc
        else
          (Any.set idx_arr oc @@ Stdint.Uint64.of_int acc;
          Any.set idx_arr nc @@ Stdint.Uint64.of_int nb';
          set_partial ~append:true [acc, s] >>| fun () ->
          acc + nb')) (csize - pad) @@ ArrayMap.bindings m
    >>= fun bsize ->
    let ib = encode_index_chain t.index_codecs idx_arr in
    match t.index_location with
    | Start -> set_partial [0, ib]
    | End -> set_partial ~append:true [bsize, ib]

  let partial_decode t get_partial csize repr pairs =
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let is = index_size t.index_codecs cps in
    let l, pad =
      match t.index_location with
      | Start -> get_partial [0, Some is], is
      | End -> get_partial [csize - is, None], 0 in
    l >>= fun xs ->
    let idx_arr = fst @@ decode_index t cps @@ List.hd xs in
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let m =
      List.fold_left
        (fun acc (i, y) ->
          let id, c = RegularGrid.index_coord_pair grid y in
          ArrayMap.add_to_list id (i, c) acc) ArrayMap.empty pairs in
    let inner = {repr with shape = t.chunk_shape} in
    Deferred.concat_map
      (fun (idx, z) ->
        let oc = Array.append idx [|0|] in
        let nc = Array.append idx [|1|] in
        let ofs = Stdint.Uint64.to_int @@ Any.get idx_arr oc in
        let nb = Stdint.Uint64.to_int @@ Any.get idx_arr nc in
        get_partial [pad + ofs, Some nb] >>| fun l ->
        let arr = decode_chain t.codecs inner @@ List.hd l in
        List.map (fun (i, c) -> (i, Ndarray.get arr c)) z) (ArrayMap.bindings m)
end
