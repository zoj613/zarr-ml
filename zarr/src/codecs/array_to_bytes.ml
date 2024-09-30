open Array_to_array
open Bytes_to_bytes
open Codecs_intf

module Indexing = Ndarray.Indexing
module ArrayMap = Util.ArrayMap
module RegularGrid = Extensions.RegularGrid

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/bytes/v1.0.html *)
module BytesCodec = struct
  let encoded_size (input_size : int) = input_size

  let endian_module = function
    | LE -> (module Ebuffer.Little : Ebuffer.S)
    | BE -> (module Ebuffer.Big : Ebuffer.S)

  let encode : type a. a Ndarray.t -> endianness -> string = fun x t ->
    let open (val endian_module t) in
    let buf = Bytes.create @@ Ndarray.byte_size x in
    match Ndarray.data_type x with
    | Char -> Ndarray.iteri (set_char buf) x; Bytes.unsafe_to_string buf
    | Bool -> Ndarray.iteri (set_bool buf) x; Bytes.unsafe_to_string buf
    | Uint8 -> Ndarray.iteri (set_uint8 buf) x; Bytes.unsafe_to_string buf
    | Int8 -> Ndarray.iteri (set_int8 buf) x; Bytes.unsafe_to_string buf
    | Int16 -> Ndarray.iteri (set_int16 buf) x; Bytes.unsafe_to_string buf
    | Uint16 -> Ndarray.iteri (set_uint16 buf) x; Bytes.unsafe_to_string buf
    | Int32 -> Ndarray.iteri (set_int32 buf) x; Bytes.unsafe_to_string buf
    | Int64 -> Ndarray.iteri (set_int64 buf) x; Bytes.unsafe_to_string buf
    | Uint64 -> Ndarray.iteri (set_uint64 buf) x; Bytes.unsafe_to_string buf
    | Float32 -> Ndarray.iteri (set_float32 buf) x; Bytes.unsafe_to_string buf
    | Float64 -> Ndarray.iteri (set_float64 buf) x; Bytes.unsafe_to_string buf
    | Complex32 -> Ndarray.iteri (set_complex32 buf) x; Bytes.unsafe_to_string buf
    | Complex64 -> Ndarray.iteri (set_complex64 buf) x; Bytes.unsafe_to_string buf
    | Int -> Ndarray.iteri (set_int buf) x; Bytes.unsafe_to_string buf
    | Nativeint -> Ndarray.iteri (set_nativeint buf) x; Bytes.unsafe_to_string buf

  let decode :
    type a. string -> a array_repr -> endianness -> a Ndarray.t
    = fun str decoded t ->
    let open (val endian_module t) in
    let k, shp = decoded.kind, decoded.shape in
    let buf = Bytes.unsafe_of_string str in
    match k, Ndarray.dtype_size k with
    | Char, _ -> Ndarray.init k shp @@ get_char buf
    | Bool, _ -> Ndarray.init k shp @@ get_bool buf
    | Uint8, _ -> Ndarray.init k shp @@ get_int8 buf
    | Int8, _ -> Ndarray.init k shp @@ get_uint8 buf
    | Int16, s -> Ndarray.init k shp @@ fun i -> get_int16 buf (i*s)
    | Uint16, s -> Ndarray.init k shp @@ fun i -> get_uint16 buf (i*s)
    | Int32, s -> Ndarray.init k shp @@ fun i -> get_int32 buf (i*s)
    | Int64, s -> Ndarray.init k shp @@ fun i -> get_int64 buf (i*s)
    | Uint64, s -> Ndarray.init k shp @@ fun i -> get_uint64 buf (i*s)
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

module rec ArrayToBytes : sig
  val parse : arraytobytes -> int array -> unit
  val encoded_size : int -> fixed_arraytobytes -> int
  val encode : arraytobytes -> 'a Ndarray.t -> string
  val decode : arraytobytes -> 'a array_repr -> string -> 'a Ndarray.t
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
  val encode : t -> 'a Ndarray.t -> string
  val decode : t -> 'a array_repr -> string -> 'a Ndarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
  val encode_chain :
    (arraytobytes, bytestobytes) internal_chain -> 'a Ndarray.t -> string
  val decode_chain :
    (arraytobytes, bytestobytes) internal_chain -> 'a array_repr -> string -> 'a Ndarray.t
  val decode_index :
    t -> int array -> string -> Stdint.uint64 Ndarray.t * string
  val index_size : 
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain -> int array -> int
  val encode_index_chain :
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain -> Stdint.uint64 Ndarray.t -> string
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
    Stdint.uint64 Ndarray.t ->
    string
    = fun t x ->
    let y = match t.a2a with
      | [] -> x
      | `Transpose o :: _ -> Ndarray.transpose ~axis:o x in
    let z = match t.a2b with
      | `Bytes e -> BytesCodec.encode y e in
    List.fold_left BytesToBytes.encode z (t.b2b :> bytestobytes list)

  let encode : type a. t -> a Ndarray.t -> string = fun t x ->
    let shard_shape = Ndarray.shape x in
    let cps = Array.map2 (/) shard_shape t.chunk_shape in
    let idx_shp = Array.append cps [|2|] in
    let shard_idx = Ndarray.create Uint64 idx_shp Stdint.Uint64.max_int in
    let grid = RegularGrid.create ~array_shape:shard_shape t.chunk_shape in
    let kind = Ndarray.data_type x in
    let m =
      Array.fold_right
        (fun y acc ->
          let k, c = RegularGrid.index_coord_pair grid y in
          ArrayMap.add_to_list k (c, Ndarray.get x y) acc)
        (Indexing.coords_of_slice [||] shard_shape) ArrayMap.empty
    in
    let xs =
      snd @@
      ArrayMap.fold
        (fun idx pairs (ofs, xs) ->
          let v = Array.of_list @@ snd @@ List.split pairs in
          let x' = Ndarray.of_array kind t.chunk_shape v in
          let b = encode_chain t.codecs x' in
          let nb = Stdint.Uint64.of_int @@ String.length b in
          Ndarray.set shard_idx (Array.append idx [|0|]) ofs;
          Ndarray.set shard_idx (Array.append idx [|1|]) nb;
          Stdint.Uint64.(ofs + nb), b :: xs) m @@ (Stdint.Uint64.zero, [])
    in
    let ib = encode_index_chain t.index_codecs shard_idx in
    match t.index_location with
    | Start -> String.concat String.empty @@ ib :: List.rev xs
    | End -> String.concat String.empty @@ List.rev @@ ib :: xs

  let decode_chain t repr x =
    let shape = List.fold_left ArrayToArray.encoded_repr repr.shape t.a2a in
    List.fold_right ArrayToArray.decode t.a2a @@
    ArrayToBytes.decode t.a2b {repr with shape} @@
    List.fold_right BytesToBytes.decode t.b2b x

  let decode_index_chain :
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain ->
    int array ->
    string ->
    Stdint.uint64 Ndarray.t
    = fun t shape x ->
    let shape' = List.fold_left ArrayToArray.encoded_repr shape t.a2a in
    let y = List.fold_right BytesToBytes.decode (t.b2b :> bytestobytes list) x in
    let arr = match t.a2b with
      | `Bytes e -> BytesCodec.decode y {shape=shape'; kind=Uint64} e in
    match t.a2a with
    | [] -> arr
    | `Transpose o :: _ ->
      let inv_order = Array.(make (length o) 0) in
      Array.iteri (fun i x -> inv_order.(x) <- i) o;
      Ndarray.transpose ~axis:inv_order arr

  let index_size index_chain cps =
    encoded_size (16 * Util.prod cps) index_chain

  let decode_index t cps b =
    let l = index_size t.index_codecs cps in
    let o = String.length b - l in
    let ib, rest = match t.index_location with
      | End -> String.sub b o l, String.sub b 0 o
      | Start -> String.sub b 0 l, String.sub b l o in
    decode_index_chain t.index_codecs (Array.append cps [|2|]) ib, rest

  let decode : type a. t -> a array_repr -> string -> a Ndarray.t = fun t repr b ->
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let idx_arr, b' = decode_index t cps b in
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let icoords =
      Array.mapi
        (fun i v -> i, v) @@ Indexing.coords_of_slice [||] repr.shape in
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
          let ofs = Stdint.Uint64.to_int @@ Ndarray.get idx_arr oc in
          let nb = Stdint.Uint64.to_int @@ Ndarray.get idx_arr nc in
          let arr = decode_chain t.codecs inner @@ String.sub b' ofs nb in
          List.map (fun (i, c) -> i, Ndarray.get arr c) pairs)
        (ArrayMap.bindings m)
    in Ndarray.of_array inner.kind repr.shape v

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
        Result.fold ~ok:(fun v -> v :: l, r) ~error:(fun _ -> l, c :: r) @@ f c)
        encoded ([], []) in
    let* codecs = match codecs with
      | [] -> Error "No codec chain specified for sharding_indexed."
      | y -> Ok y
    in
    let* a2b, rest =
      match filter_partition (ArrayToBytes.of_yojson chunk_shape) codecs with
      | [x], rest -> Ok (x, rest)
      | _ -> Error "Must be exactly one array->bytes codec."
    in
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
    let* l' = match extract "chunk_shape" with
      | [] -> Error ("sharding_indexed must contain a chunk_shape field")
      | x :: _ ->
        List.fold_right (fun a acc ->
          let* k = acc in
          match a with
          | `Int i when i > 0 -> Ok (i :: k)
          | _ -> Error "chunk_shape must only contain positive integers.")
          (Yojson.Safe.Util.to_list x) (Ok [])
    in
    let chunk_shape = Array.of_list l' in
    let* index_location = match extract "index_location" with
      | [] -> Error "sharding_indexed must have a index_location field"
      | x :: _ ->
        match x with
        | `String "end" -> Ok End
        | `String "start" -> Ok Start
        | _ -> Error "index_location must only be 'end' or 'start'"
    in
    let* codecs = match extract "codecs" with
      | [] -> Error "sharding_indexed must have a codecs field"
      | x :: _ ->
        chain_of_yojson chunk_shape @@ Yojson.Safe.Util.to_list x
    in
    let* ic = match extract "index_codecs" with
      | [] -> Error "sharding_indexed must have a index_codecs field"
      | x :: _ ->
        let cps = Array.map2 (/) shard_shape chunk_shape in
        let idx_shape = Array.append cps [|2|] in
        chain_of_yojson idx_shape @@ Yojson.Safe.Util.to_list x
    in
    (* Ensure index_codecs only contains fixed size
       array->bytes and bytes->bytes codecs. *)
    let msg = "index_codecs must not contain variable-sized codecs." in
    let* b2b = List.fold_right
      (fun c acc ->
        let* l = acc in
        match c with
        | `Crc32c -> Ok (`Crc32c :: l)
        | `Gzip _ | `Zstd _ -> Error msg) ic.b2b (Ok [])
    in
    let+ a2b = match ic.a2b with
      | `Bytes e -> Ok (`Bytes e)
      | `ShardingIndexed _ -> Error msg
    in {index_codecs = {ic with a2b; b2b}; index_location; codecs; chunk_shape}
end

module Make (Io : Types.IO) = struct
  open Io
  open Deferred.Syntax
  open ShardingIndexedCodec

  type t = ShardingIndexedCodec.t
  type set_fn = ?append:bool -> (int * string) list -> unit Deferred.t

  let add_binding ~grid acc (c, v) =
    let id, co = RegularGrid.index_coord_pair grid c in
    ArrayMap.add_to_list id (co, v) acc

  (* specialized function for partially writing possibly multiple inner chunks
     to an empty shard of a designated array using the sharding indexed codec.*)
  let partial_encode_empty_shard t (set_partial : set_fn) repr pairs fill_value =
    let update_index ~index i z (ofs, acc) =
      let arr = Ndarray.create repr.kind t.chunk_shape fill_value in
      List.iter (fun (c, v) -> Ndarray.set arr c v) z;
      let s = encode_chain t.codecs arr in
      let n = String.length s in
      Ndarray.set index (Array.append i [|0|]) @@ Stdint.Uint64.of_int ofs;
      Ndarray.set index (Array.append i [|1|]) @@ Stdint.Uint64.of_int n;
      ofs + n, (ofs, s) :: acc
    in
    (* simulate the inner chunks of a shard as a regular grid of specified shape.*)
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    (* build a finite map with its keys being an inner chunk's index and values
       being a list of (coord-within-inner-chunk, new-value) pairs such that
       new-value is set for the coordinate coord-within-inner-chunk of the inner
       chunk represented by the associated key.*)
    let m = List.fold_left (add_binding ~grid) ArrayMap.empty pairs in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let init = match t.index_location with
      | Start -> index_size t.index_codecs cps
      | End -> 0
    in
    let index = Ndarray.create Uint64 (Array.append cps [|2|]) Stdint.Uint64.max_int in
    let shard_size, ranges = ArrayMap.fold (update_index ~index) m (init, []) in
    let indexbytes = encode_index_chain t.index_codecs index in
    (* write all resultant (offset, bytes) pairs into the bytes of the new shard
       taking note to append/prepend the bytes of the shard's index array.*)
    match t.index_location with
    | Start -> set_partial @@ (0, indexbytes) :: List.rev ranges
    | End -> set_partial @@ List.rev @@ (shard_size, indexbytes) :: ranges

  (* function to partially write new values to one or more inner chunks of
     an existing shard using the sharding indexed codec. *)
  let partial_encode t get_partial (set_partial : set_fn) shardsize repr pairs fv =
    if shardsize = 0 then partial_encode_empty_shard t set_partial repr pairs fv else
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let is = index_size t.index_codecs cps in
    let* l = match t.index_location with
      | Start -> get_partial [0, Some is]
      | End -> get_partial [shardsize - is, None]
    in
    let idx_arr = fst @@ decode_index t cps @@ List.hd l in
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let m = List.fold_left (add_binding ~grid) ArrayMap.empty pairs in
    (* split the finite map m into key-value pairs representing empty inner chunks
       and those that don't (using the fact that empty inner chunks have index
       array values equal to 2^64 - 1; then process these seperately.*)
    let empty, nonempty = List.partition_map
      (fun ((i, _) as bd) ->
        let oc = Array.append i [|0|] and nc = Array.append i [|1|] in
        match Ndarray.(get idx_arr oc, get idx_arr nc) with
        | o, n when Stdint.Uint64.(max_int = o && max_int = n) ->
          Either.Left ((-1, None), (oc, nc, -1, 0, bd))
        | o, n ->
          let o', n' = Stdint.Uint64.(to_int o, to_int n) in
          Either.Right ((o', Some n'), (oc, nc, o', n', bd))) ArrayMap.(bindings m)
    in
    let ranges, nonempty' = List.split nonempty in
    (* fold over the nonempty index coordinates and finite map to obtain
       (offset, bytes) pairs to write in-place and those to append at the
       end of the shard. bytes to write in-place are determined by comparing
       encoded size vs the corresponding nbytes[i] value already contained in
       the shard's index array.*)
    let* xs = get_partial ranges in
    let repr' = {repr with shape = t.chunk_shape} in
    let bsize, inplace, nonempty_append = List.fold_left2
      (fun (acc, l, r) x (oc, nc, ofs, nb, (_, z)) -> 
        let arr = decode_chain t.codecs repr' x in
        List.iter (fun (c, v) -> Ndarray.set arr c v) z;
        let s = encode_chain t.codecs arr in
        let nb' = String.length s in
        if nb' = nb then acc, (ofs, s) :: l, r else begin
          Ndarray.set idx_arr oc @@ Stdint.Uint64.of_int acc;
          Ndarray.set idx_arr nc @@ Stdint.Uint64.of_int nb';
          acc + nb', l, (acc, s) :: r end) (shardsize, [], []) xs nonempty'
    in
    let* () = match inplace with
      | [] -> Deferred.return_unit
      | rs -> set_partial rs
    in
    let* () = match nonempty_append with
      | [] -> Deferred.return_unit
      | rs -> set_partial ~append:true @@ List.rev rs
    in
    (* new values that need to be written to previously empty inner chunks will
       be appended at the end of the shard and the corresponding index array's
       offset and number-of-bytes values updated accordingly.*)
    let bsize', empty_append = List.fold_left
      (fun (ofs, l) (_, (oc, nc, _, _, (_, z))) -> 
        let arr = Ndarray.create repr'.kind repr'.shape fv in
        List.iter (fun (c, v) -> Ndarray.set arr c v) z;
        let s = encode_chain t.codecs arr in
        let n = String.length s in
        Ndarray.set idx_arr oc @@ Stdint.Uint64.of_int ofs;
        Ndarray.set idx_arr nc @@ Stdint.Uint64.of_int n;
        ofs + n, (ofs, s) :: l) (bsize, []) empty
    in
    let* () = match empty_append with
      | [] -> Deferred.return_unit
      | rs -> set_partial ~append:true @@ List.rev rs
    in
    let ib = encode_index_chain t.index_codecs idx_arr in
    match t.index_location with
    | Start -> set_partial [0, ib]
    | End -> set_partial ~append:true [bsize', ib]

  (* function to partially read values off of a non-empty shard previously
     encoded using the sharding indexed codec. *) 
  let partial_decode t get_partial shardsize repr pairs fill_value =
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let add_binding ~grid acc (i, y) =
      let id, c = RegularGrid.index_coord_pair grid y in
      ArrayMap.add_to_list id (i, c) acc
    in
    let m = List.fold_left (add_binding ~grid) ArrayMap.empty pairs in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let is = index_size t.index_codecs cps in
    let* l = match t.index_location with
      | Start -> get_partial [0, Some is]
      | End -> get_partial [shardsize - is, None]
    in
    let index = fst @@ decode_index t cps @@ List.hd l in
    let empty, nonempty = List.partition_map
      (fun ((i, _) as bd) ->
        match Ndarray.(get index @@ Array.append i [|0|],
                       get index @@ Array.append i [|1|]) with
        | o, n when Stdint.Uint64.(max_int = o && max_int = n) ->
          Either.Left ((-1, None), bd)
        | o, n ->
          let o', n' = Stdint.Uint64.(to_int o, to_int n) in
          Either.Right ((o', Some n'), bd)) ArrayMap.(bindings m)
    in
    let ranges, bindings = List.split nonempty in
    let+ xs = get_partial ranges in
    let repr' = {repr with shape = t.chunk_shape} in
    let res1 = List.concat @@ List.map2
      (fun x (_, z) ->
        let arr = decode_chain t.codecs repr' x in
        List.map (fun (i, c) -> i, Ndarray.get arr c) z) xs bindings
    in
    let res2 = List.concat_map
      (fun (_, (_, z)) -> List.map (fun (i, _) -> i, fill_value) z) empty
    in  
    res1 @ res2
end
