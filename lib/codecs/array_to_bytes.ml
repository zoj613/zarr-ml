open Array_to_array
open Bytes_to_bytes
open Util.Result_syntax
open Codecs_intf
open Util

module Ndarray = Owl.Dense.Ndarray.Generic

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/bytes/v1.0.html *)
module BytesCodec = struct
  let compute_encoded_size (input_size : int) = input_size

  let endian_module = function
    | LE -> (module Ebuffer.Little : Ebuffer.S)
    | BE -> (module Ebuffer.Big : Ebuffer.S)

  let encode :
    type a b. (a, b) Ndarray.t -> endianness -> (string, [> error]) result
    = fun x t ->
    let open Bigarray in
    let open (val endian_module t) in
    let buf = Buffer.create @@ Ndarray.size_in_bytes x in
    match Ndarray.kind x with
    | Char -> Ndarray.iter (add_char buf) x; Ok (contents buf)
    | Int8_signed -> Ndarray.iter (add_int8 buf) x; Ok (contents buf)
    | Int8_unsigned -> Ndarray.iter (add_uint8 buf) x; Ok (contents buf)
    | Int16_signed -> Ndarray.iter (add_int16 buf) x; Ok (contents buf)
    | Int16_unsigned -> Ndarray.iter (add_uint16 buf) x; Ok (contents buf)
    | Int32 -> Ndarray.iter (add_int32 buf) x; Ok (contents buf)
    | Int64 -> Ndarray.iter (add_int64 buf) x; Ok (contents buf)
    | Float32 -> Ndarray.iter (add_float32 buf) x; Ok (contents buf)
    | Float64 -> Ndarray.iter (add_float64 buf) x; Ok (contents buf)
    | Complex32 -> Ndarray.iter (add_complex32 buf) x; Ok (contents buf) 
    | Complex64 -> Ndarray.iter (add_complex64 buf) x; Ok (contents buf) 
    | Int -> Ndarray.iter (add_int buf) x; Ok (contents buf)
    | Nativeint -> Ndarray.iter (add_nativeint buf) x; Ok (contents buf)

  let decode :
    type a b.
    string ->
    (a, b) array_repr ->
    endianness ->
    ((a, b) Ndarray.t, [> `Store_read of string | error]) result
    = fun buf decoded t ->
    let open Bigarray in
    let open (val endian_module t) in
    let k, shp = decoded.kind, decoded.shape in
    match k, kind_size_in_bytes k with
    | Char, _ -> Ok (Ndarray.init k shp @@ get_char buf)
    | Int8_signed, _ -> Ok (Ndarray.init k shp @@ get_int8 buf)
    | Int8_unsigned, _ -> Ok (Ndarray.init k shp @@ get_uint8 buf)
    | Int16_signed, s -> Ok (Ndarray.init k shp @@ fun i -> get_int16 buf (i*s))
    | Int16_unsigned, s -> Ok (Ndarray.init k shp @@ fun i -> get_uint16 buf (i*s))
    | Int32, s -> Ok (Ndarray.init k shp @@ fun i -> get_int32 buf (i*s))
    | Int64, s -> Ok (Ndarray.init k shp @@ fun i -> get_int64 buf (i*s))
    | Float32, s -> Ok (Ndarray.init k shp @@ fun i -> get_float32 buf (i*s))
    | Float64, s -> Ok (Ndarray.init k shp @@ fun i -> get_float64 buf (i*s))
    | Complex32, s -> Ok (Ndarray.init k shp @@ fun i -> get_complex32 buf (i*s))
    | Complex64, s -> Ok (Ndarray.init k shp @@ fun i -> get_complex64 buf (i*s))
    | Int, s -> Ok (Ndarray.init k shp @@ fun i -> get_int buf (i*s))
    | Nativeint, s -> Ok (Ndarray.init k shp @@ fun i -> get_nativeint buf (i*s))

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
      | s -> Error ("Unsupported bytes endianness: " ^ s))
    | _ -> Error "Invalid bytes codec configuration."
end

module Any = Owl.Dense.Ndarray.Any

module rec ArrayToBytes : sig
  val parse : arraytobytes -> int array -> (unit, [> error]) result
  val compute_encoded_size : int -> fixed_arraytobytes -> int
  val encode :
    arraytobytes ->
    ('a, 'b) Ndarray.t ->
    (string, [> error]) result
  val decode :
    arraytobytes ->
    ('a, 'b) array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> `Store_read of string | error]) result
  val of_yojson : int array -> Yojson.Safe.t -> (arraytobytes, string) result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end = struct

  let parse t shp =
    match t with
    | `Bytes _ -> Ok ()
    | `ShardingIndexed c -> ShardingIndexedCodec.parse c shp

  let compute_encoded_size :
    int -> fixed_arraytobytes -> int
    = fun input_size -> function
    | `Bytes _ -> BytesCodec.compute_encoded_size input_size

  let encode :
    arraytobytes ->
    ('a, 'b) Ndarray.t ->
    (string, [> error]) result
    = fun t x -> match t with
    | `Bytes endian -> BytesCodec.encode x endian
    | `ShardingIndexed c -> ShardingIndexedCodec.encode c x

  let decode :
    arraytobytes ->
    ('a, 'b) array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> error]) result
    = fun t repr b ->
    match t with
    | `Bytes endian -> BytesCodec.decode b repr endian
    | `ShardingIndexed c -> ShardingIndexedCodec.decode c repr b

  let to_yojson = function
    | `Bytes endian -> BytesCodec.to_yojson endian
    | `ShardingIndexed c -> ShardingIndexedCodec.to_yojson c

  let of_yojson shp x =
    match Util.get_name x with
    | "bytes" -> BytesCodec.of_yojson x >>| fun e -> `Bytes e
    | "sharding_indexed" ->
      ShardingIndexedCodec.of_yojson shp x >>| fun c -> `ShardingIndexed c
    | _ -> Error ("array->bytes codec not supported: ")
end

and ShardingIndexedCodec : sig
  type t = internal_shard_config
  val parse : t -> int array -> (unit, [> error]) result
  val encode : t -> ('a, 'b) Ndarray.t -> (string, [> error]) result
  val partial_encode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    partial_setter ->
    int ->
    ('a, 'b) array_repr ->
    (int array * 'a) list ->
    (unit, 'c) result
  val partial_decode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    int ->
    ('a, 'b) array_repr ->
    (int * int array) list ->
    ((int * 'a) list, 'c) result
  val decode :
    t ->
    ('a, 'b) array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> `Store_read of string | error]) result
  val of_yojson : int array -> Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
end = struct
  type t = internal_shard_config  

  let parse_chain : int array -> (arraytobytes, bytestobytes) internal_chain ->
    (unit, [> error ]) result
    = fun shape chain ->
    ArrayToBytes.parse chain.a2b @@
    (match chain.a2a with
    | [] -> shape
    | l ->
      ArrayToArray.parse (List.hd l) shape;
      List.fold_left ArrayToArray.compute_encoded_representation shape l)

  let parse :
    internal_shard_config ->
    int array ->
    (unit, [> error]) result
    = fun t shape ->
    (match Array.(length shape = length t.chunk_shape) with
    | true -> Ok ()
    | false ->
      let msg = "chunk_shape size must equal the dimensionality of its shard." in
      Result.error @@ `Sharding (t.chunk_shape, shape, msg))
    >>= fun () ->
    (match
      Array.for_all2 (fun x y -> (x mod y) = 0) shape t.chunk_shape
    with
    | true -> Ok ()
    | false ->
      let msg = "chunk_shape must evenly divide the size of the shard shape." in
      Result.error @@ `Sharding (t.chunk_shape, shape, msg))
    >>= fun () ->
    parse_chain shape t.codecs >>= fun () ->
    parse_chain
      (Array.append shape [|2|])
      (t.index_codecs :> (arraytobytes, bytestobytes) internal_chain) 

  let compute_encoded_size input_size chain =
    List.fold_left BytesToBytes.compute_encoded_size
      (ArrayToBytes.compute_encoded_size
         (List.fold_left
            ArrayToArray.compute_encoded_size
            input_size chain.a2a) chain.a2b) chain.b2b
  
  let encode_chain :
    (arraytobytes, bytestobytes) internal_chain ->
    ('a, 'b) Ndarray.t ->
    (string, [> error]) result
    = fun t x ->
    ArrayToBytes.encode t.a2b @@ List.fold_left ArrayToArray.encode x t.a2a
    >>| fun y -> List.fold_left BytesToBytes.encode y t.b2b

  let encode_index_chain :
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain ->
    Stdint.uint64 Any.arr ->
    (string, [> error]) result
    = fun t x ->
    let open Stdint in
    (match t.a2a with
    | [] -> Ok x
    | `Transpose o :: _ -> Result.ok @@ Any.transpose ~axis:o x)
    >>= fun y ->
    let buf = Bytes.create @@ 8 * Any.numel y in
    (match t.a2b with
    | `Bytes LE ->
      Any.iteri (fun i v -> Uint64.to_bytes_little_endian v buf (i*8)) y;
      Ok (Bytes.to_string buf)
    | `Bytes BE ->
      Any.iteri (fun i v -> Uint64.to_bytes_big_endian v buf (i*8)) y;
      Ok (Bytes.to_string buf))
    >>| fun z ->
    List.fold_left BytesToBytes.encode z (t.b2b :> bytestobytes list)

  let encode :
    internal_shard_config ->
    ('a, 'b) Ndarray.t ->
    (string, [> error]) result
    = fun t x ->
    let open Extensions in
    let open Stdint in
    let shard_shape = Ndarray.shape x in
    let cps = Array.map2 (/) shard_shape t.chunk_shape in
    let idx_shp = Array.append cps [|2|] in
    let shard_idx = Any.create idx_shp Uint64.max_int in
    let grid = RegularGrid.create ~array_shape:shard_shape t.chunk_shape in
    let slice = Array.make (Ndarray.num_dims x) @@ Owl_types.R [] in
    let m =
      Array.fold_right
        (fun y acc ->
          let k, c = RegularGrid.index_coord_pair grid y in
          ArrayMap.add_to_list k (c, Ndarray.get x y) acc)
        (Indexing.coords_of_slice slice shard_shape) ArrayMap.empty
    in
    let kind = Ndarray.kind x in
    ArrayMap.fold
      (fun idx pairs acc ->
        acc >>= fun (offset, xs) ->
        let v = Array.of_list @@ snd @@ List.split pairs in
        let x' = Ndarray.of_array kind v t.chunk_shape in
        encode_chain t.codecs x' >>| fun b ->
        let nb = Uint64.of_int @@ String.length b in
        Any.set shard_idx (Array.append idx [|0|]) offset;
        Any.set shard_idx (Array.append idx [|1|]) nb;
        Uint64.(offset + nb), b :: xs) m @@ Ok (Uint64.zero, [])
    >>= fun (_, xs) ->
    encode_index_chain t.index_codecs shard_idx >>| fun ib ->
    match t.index_location with
    | Start -> String.concat "" @@ ib :: List.rev xs
    | End -> String.concat "" @@ List.rev @@ ib :: xs

  let decode_chain :
    (arraytobytes, bytestobytes) internal_chain ->
    ('a, 'b) array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> error]) result
    = fun t repr x ->
    let shape =
      List.fold_left
        ArrayToArray.compute_encoded_representation repr.shape t.a2a in
    let y = List.fold_right BytesToBytes.decode t.b2b x in
    ArrayToBytes.decode t.a2b {repr with shape} y >>| fun a ->
    List.fold_right ArrayToArray.decode t.a2a a

  let decode_index_chain :
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain ->
    int array ->
    string ->
    (Stdint.uint64 Any.arr, [> error]) result
    = fun t shape x ->
    let open Stdint in
    let shape' =
      List.fold_left ArrayToArray.compute_encoded_representation shape t.a2a in
    let y =
      List.fold_right BytesToBytes.decode (t.b2b :> bytestobytes list) x in
    let buf = Bytes.of_string y in
    let arr =
      match t.a2b with
      | `Bytes LE ->
        Any.init shape' @@ fun i -> Uint64.of_bytes_little_endian buf (i*8)
      | `Bytes BE ->
        Any.init shape' @@ fun i -> Uint64.of_bytes_big_endian buf (i*8)
    in
    match t.a2a with
    | [] -> Ok arr
    | `Transpose o :: _ ->
      let inv_order = Array.(make (length o) 0) in
      Array.iteri (fun i x -> inv_order.(x) <- i) o;
      Result.ok @@ Any.transpose ~axis:inv_order arr

  let index_size :
    (fixed_arraytobytes, fixed_bytestobytes) internal_chain ->
    int array ->
    int
    = fun index_chain cps ->
    compute_encoded_size (16 * Util.prod cps) index_chain

  let decode_index :
    internal_shard_config ->
    int array ->
    string ->
    (Stdint.uint64 Any.arr * string, [> error]) result
    = fun t cps b ->
    let l = index_size t.index_codecs cps in
    let o = String.length b - l in
    let index_bytes, rest =
      match t.index_location with
      | End -> String.sub b o l, String.sub b 0 o
      | Start -> String.sub b 0 l, String.sub b l o
    in
    decode_index_chain t.index_codecs (Array.append cps [|2|]) index_bytes
    >>| fun decoded -> decoded, rest

  let partial_encode :
    internal_shard_config ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ]) result) ->
    partial_setter ->
    int ->
    ('a, 'b) array_repr ->
    (int array * 'a) list ->
    (unit, [> `Store_read of string | error ]) result
    = fun t get_partial set_partial bytesize repr pairs ->
    let open Extensions in
    let open Stdint in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let is = index_size t.index_codecs cps in
    let ibytes, pad =
      match t.index_location with
      | Start -> get_partial [0, Some is], is
      | End -> get_partial [bytesize - is, None], 0 in
    ibytes >>= fun xs ->
    decode_index t cps @@ List.hd xs >>= fun (idx_arr, _) ->
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let m =
      List.fold_left
        (fun acc (c, v) ->
          let id, co = RegularGrid.index_coord_pair grid c in
          ArrayMap.add_to_list id (co, v) acc) ArrayMap.empty pairs
    in
    let inner = {repr with shape = t.chunk_shape} in
    ArrayMap.fold
      (fun idx z acc ->
        acc >>= fun (bsize, shard_idx) ->
        let ofs_coord = Array.append idx [|0|] in
        let nb_coord = Array.append idx [|1|] in
        let offset = Any.get shard_idx ofs_coord |> Uint64.to_int in
        let nb = Any.get shard_idx nb_coord |> Uint64.to_int in
        get_partial [pad + offset, Some nb] >>= fun l ->
        decode_chain t.codecs inner @@ List.hd l >>= fun arr ->
        List.iter (fun (c, v) -> Ndarray.set arr c v) z;
        encode_chain t.codecs arr >>| fun s ->
        let nb' = String.length s in
        (* if codec chain doesnt contain compressions so update chunk in-place *)
        if nb' = nb then
          (set_partial [pad + offset, s]; bsize, shard_idx)
        else
          (Any.set shard_idx ofs_coord @@ Uint64.of_int bsize;
          Any.set shard_idx nb_coord @@ Uint64.of_int nb';
          set_partial ~append:true [bsize, s];
          bsize + nb', shard_idx)) m (Ok (bytesize - pad, idx_arr))
    >>= fun (bytesize, shard_idx) ->
    encode_index_chain t.index_codecs shard_idx >>| fun ib ->
    match t.index_location with
    | Start -> set_partial [0, ib]
    | End -> set_partial ~append:true [bytesize, ib]

  let decode :
    internal_shard_config ->
    ('a, 'b) array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> `Store_read of string | error]) result
    = fun t repr b ->
    let open Extensions in
    let open Stdint in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    decode_index t cps b >>= fun (shard_idx, b') ->
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let slice = Array.make (Array.length repr.shape) @@ Owl_types.R [] in
    let icoords =
      Array.mapi
        (fun i v -> i, v) @@ Indexing.coords_of_slice slice repr.shape
    in
    let m =
      Array.fold_left
        (fun acc (i, y) ->
          let k, c = RegularGrid.index_coord_pair grid y in
          ArrayMap.add_to_list k (i, c) acc) ArrayMap.empty icoords
    in
    let inner = {repr with shape = t.chunk_shape} in
    ArrayMap.fold
      (fun idx pairs acc ->
        acc >>= fun xs ->
        let ofs = Any.get shard_idx @@ Array.append idx [|0|] |> Uint64.to_int in
        let nb = Any.get shard_idx @@ Array.append idx [|1|] |> Uint64.to_int in
        decode_chain t.codecs inner @@ String.sub b' ofs nb >>| fun arr ->
        List.fold_left
          (fun a (i, c) -> (i, Ndarray.get arr c) :: a) xs pairs) m (Ok [])
    >>| fun pairs ->
    let v =
      Array.of_list @@ snd @@ List.split @@
      List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) pairs in
    Ndarray.of_array inner.kind v repr.shape

  let partial_decode :
    internal_shard_config ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ]) result) ->
    int ->
    ('a, 'b) array_repr ->
    (int * int array) list ->
    ((int * 'a) list, [> `Store_read of string | error ]) result
    = fun t get_partial bsize repr pairs ->
    let open Extensions in
    let open Stdint in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let is = index_size t.index_codecs cps in
    let ibytes, pad =
      match t.index_location with
      | Start -> get_partial [0, Some is], is
      | End -> get_partial [bsize - is, None], 0 in
    ibytes >>= fun xs ->
    decode_index t cps @@ List.hd xs >>= fun (idx_arr, _) ->
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let m =
      List.fold_left
        (fun acc (i, y) ->
          let id, c = RegularGrid.index_coord_pair grid y in
          ArrayMap.add_to_list id (i, c) acc) ArrayMap.empty pairs
    in
    let inner = {repr with shape = t.chunk_shape} in
    ArrayMap.fold
      (fun idx z acc ->
        acc >>= fun (shard_idx, xs) ->
        let ofs = Any.get shard_idx @@ Array.append idx [|0|] |> Uint64.to_int in
        let nb = Any.get shard_idx @@ Array.append idx [|1|] |> Uint64.to_int in
        get_partial [pad + ofs, Some nb] >>= fun l ->
        decode_chain t.codecs inner @@ List.hd l >>| fun arr ->
        shard_idx, xs @ List.map (fun (i, c) -> (i, Ndarray.get arr c)) z)
      m @@ Ok (idx_arr, [])
    >>| snd

  let chain_to_yojson chain =
    `List
      (List.map ArrayToArray.to_yojson chain.a2a @
      (ArrayToBytes.to_yojson chain.a2b) ::
      List.map BytesToBytes.to_yojson chain.b2b)

  let to_yojson t =
    let codecs = chain_to_yojson t.codecs in
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
       ("codecs", codecs)])]

  let chain_of_yojson :
    int array ->
    Yojson.Safe.t list ->
    ((arraytobytes, bytestobytes) internal_chain, string) result
    = fun chunk_shape codecs -> 
    let filter_partition f encoded =
      List.fold_right (fun c (l, r) ->
        match f c with
        | Ok v -> v :: l, r
        | Error _ -> l, c :: r) encoded ([], [])
    in
    (match codecs with
    | [] ->
      Error "No codec chain specified for sharding_indexed."
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

  let of_yojson shard_shape x =
    let assoc =
      Yojson.Safe.Util.(member "configuration" x |> to_assoc)
    in
    let extract name =
      Yojson.Safe.Util.filter_map
        (fun (n, v) -> if n = name then Some v else None) assoc
    in
    (match extract "chunk_shape" with
    | [] ->
      Error ("sharding_indexed must contain a chunk_shape field")
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
    | [] ->
      Error "sharding_indexed must have a index_location field"
    | x :: _ ->
      match x with
      | `String "end" -> Ok End
      | `String "start" -> Ok Start
      | _ -> Error "index_location must only be 'end' or 'start'")
    >>= fun index_location ->
    (match extract "codecs" with
    | [] ->
      Error "sharding_indexed must have a codecs field"
    | x :: _ ->
      chain_of_yojson chunk_shape @@ Yojson.Safe.Util.to_list x)
    >>= fun codecs ->
    (match extract "index_codecs" with
    | [] ->
      Error "sharding_indexed must have a index_codecs field"
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
