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

  let mk_array kind shape init_fn =
    let x = Array.init (Util.prod shape) init_fn in
    Ok (Ndarray.of_array kind x shape)

  let decode :
    type a b.
    string ->
    (a, b) Util.array_repr ->
    endianness ->
    ((a, b) Ndarray.t, [> `Store_read of string | error]) result
    = fun buf decoded t ->
    let open Bigarray in
    let open (val endian_module t) in
    let k, shp = decoded.kind, decoded.shape in
    match k, kind_size_in_bytes k with
    | Char, _ -> mk_array k shp @@ get_char buf
    | Int8_signed, _ -> mk_array k shp @@ get_int8 buf
    | Int8_unsigned, _ -> mk_array k shp @@ get_int8 buf
    | Int16_signed, s -> mk_array k shp @@ fun i -> get_int16 buf (i*s)
    | Int16_unsigned, s -> mk_array k shp @@ fun i -> get_uint16 buf (i*s)
    | Int32, s -> mk_array k shp @@ fun i -> get_int32 buf (i*s)
    | Int64, s -> mk_array k shp @@ fun i -> get_int64 buf (i*s)
    | Float32, s -> mk_array k shp @@ fun i -> get_float32 buf (i*s)
    | Float64, s -> mk_array k shp @@ fun i -> get_float64 buf (i*s)
    | Complex32, s -> mk_array k shp @@ fun i -> get_complex32 buf (i*s)
    | Complex64, s -> mk_array k shp @@ fun i -> get_complex64 buf (i*s)
    | Int, s -> mk_array k shp @@ fun i -> get_int buf (i*s)
    | Nativeint, s -> mk_array k shp @@ fun i -> get_nativeint buf (i*s)

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
      | s ->
        Result.error @@ "Unsupported bytes endianness: " ^ s)
    | _ -> Error "Invalid bytes codec configuration."
end

module rec ArrayToBytes : sig
  val parse :
    array_tobytes ->
    ('a, 'b) Util.array_repr ->
    (unit, [> error]) result
  val compute_encoded_size : int -> array_tobytes -> int
  val default : array_tobytes
  val encode :
    array_tobytes ->
    ('a, 'b) Ndarray.t ->
    (string, [> error]) result
  val decode :
    array_tobytes ->
    ('a, 'b) Util.array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> `Store_read of string | error]) result
  val of_yojson : Yojson.Safe.t -> (array_tobytes, string) result
  val to_yojson : array_tobytes -> Yojson.Safe.t
end = struct

  let default = `Bytes LE

  let parse t decoded_repr =
    match t with
    | `Bytes _ -> Ok ()
    | `ShardingIndexed c -> ShardingIndexedCodec.parse c decoded_repr

  let compute_encoded_size input_size = function
    | `Bytes _ ->
      BytesCodec.compute_encoded_size input_size
    | `ShardingIndexed s ->
      ShardingIndexedCodec.compute_encoded_size input_size s

  let encode :
    array_tobytes ->
    ('a, 'b) Ndarray.t ->
    (string, [> error]) result
    = fun t x -> match t with
    | `Bytes endian -> BytesCodec.encode x endian
    | `ShardingIndexed c -> ShardingIndexedCodec.encode c x

  let decode :
    array_tobytes ->
    ('a, 'b) Util.array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> error]) result
    = fun t repr b ->
    match t with
    | `Bytes endian -> BytesCodec.decode b repr endian
    | `ShardingIndexed c -> ShardingIndexedCodec.decode c repr b

  let to_yojson = function
    | `Bytes endian -> BytesCodec.to_yojson endian
    | `ShardingIndexed c -> ShardingIndexedCodec.to_yojson c

  let of_yojson x =
    match Util.get_name x with
    | "bytes" -> BytesCodec.of_yojson x >>| fun e -> `Bytes e
    | "sharding_indexed" ->
      ShardingIndexedCodec.of_yojson x >>| fun c -> `ShardingIndexed c
    | _ -> Error ("array->bytes codec not supported: ")
end

and ShardingIndexedCodec : sig
  type t = internal_shard_config
  val parse : t -> ('a, 'b) Util.array_repr -> (unit, [> error]) result
  val compute_encoded_size : int -> t -> int
  val encode : t -> ('a, 'b) Ndarray.t -> (string, [> error]) result
  val partial_encode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    partial_setter ->
    int ->
    ('a, 'b) Util.array_repr ->
    (int array * 'a) list ->
    (unit, 'c) result
  val partial_decode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    int ->
    ('a, 'b) Util.array_repr ->
    (int * int array) list ->
    ((int * 'a) list, 'c) result
  val decode :
    t ->
    ('a, 'b) Util.array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> `Store_read of string | error]) result
  val of_yojson : Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
end = struct
  type t = internal_shard_config  

  let parse_chain repr chain =
    List.fold_left
      (fun acc c ->
        acc >>= fun r ->
        ArrayToArray.parse c r >>= fun () ->
        ArrayToArray.compute_encoded_representation c r)
      (Ok repr) chain.a2a
    >>= ArrayToBytes.parse chain.a2b

  let parse :
    internal_shard_config ->
    ('a, 'b) Util.array_repr ->
    (unit, [> error]) result
    = fun t r ->
    (match Array.(length r.shape = length t.chunk_shape) with
    | true -> Ok ()
    | false ->
      let msg =
        "sharding chunk_shape length must equal the dimensionality of
        the decoded representaton of a shard." in
      Result.error @@ `Sharding (t.chunk_shape, r.shape, msg))
    >>= fun () ->
    (match
      Array.for_all2 (fun x y -> (x mod y) = 0) r.shape t.chunk_shape
    with
    | true -> Ok ()
    | false ->
      let msg =
        "sharding chunk_shape must evenly divide the size of the shard shape."
      in
      Result.error @@ `Sharding (t.chunk_shape, r.shape, msg))
    >>= fun () ->
    parse_chain r t.codecs >>= fun () ->
    parse_chain {r with shape = Array.append r.shape [|2|]} t.index_codecs

  let compute_encoded_size input_size t =
    List.fold_left BytesToBytes.compute_encoded_size
      (ArrayToBytes.compute_encoded_size
         (List.fold_left
            ArrayToArray.compute_encoded_size
            input_size t.index_codecs.a2a)
         t.index_codecs.a2b)
      t.index_codecs.b2b
  
  let rec encode_chain :
    type a b.
    bytestobytes internal_chain ->
    (a, b) Ndarray.t ->
    (string, [> error]) result
    = fun t x ->
    List.fold_left
      (fun acc c -> acc >>= ArrayToArray.encode c) (Ok x) t.a2a
    >>= fun y ->
    List.fold_left
      (fun acc c -> acc >>= BytesToBytes.encode c)
      (ArrayToBytes.encode t.a2b y) t.b2b

  and encode :
    internal_shard_config ->
    ('a, 'b) Ndarray.t ->
    (string, [> error]) result
    = fun t x ->
    let open Extensions in
    let shard_shape = Ndarray.shape x in
    let cps = Array.map2 (/) shard_shape t.chunk_shape in
    let idx_shp = Array.append cps [|2|] in
    let shard_idx = Ndarray.create Bigarray.Int64 idx_shp Int64.max_int in
    RegularGrid.create ~array_shape:shard_shape t.chunk_shape >>= fun grid ->
    let slice = Array.make (Ndarray.num_dims x) @@ Owl_types.R [] in
    let coords = Indexing.coords_of_slice slice shard_shape in
    let tbl = Arraytbl.create @@ Array.length coords in
    Ndarray.iteri (fun i y ->
      let k, c = RegularGrid.index_coord_pair grid coords.(i) in
      Arraytbl.add tbl k (c, y)) x;
    let kind = Ndarray.kind x in
    let buf = Buffer.create @@ Ndarray.size_in_bytes x in
    let icoords = Array.map (fun v -> [|v; v|]) idx_shp in
    icoords.(Array.length shard_shape) <- [|0; 1|];
    ArraySet.fold
      (fun idx acc ->
        acc >>= fun offset ->
        (* find_all returns bindings in reverse order. To restore the
         * C-ordering of elements we must use List.rev. *)
        let v =
          Array.of_list @@ snd @@ List.split @@ List.rev @@
          Arraytbl.find_all tbl idx in
        let x' = Ndarray.of_array kind v t.chunk_shape in
        encode_chain t.codecs x' >>| fun b ->
        Buffer.add_string buf b;
        Array.iteri (fun i v -> icoords.(i).(0) <- v; icoords.(i).(1) <- v) idx;
        let nb = Int64.of_int @@ String.length b in
        Ndarray.set_index shard_idx icoords [|offset; nb|];
        Int64.add offset nb)
      (ArraySet.of_seq @@ Arraytbl.to_seq_keys tbl) (Ok 0L)
    >>= fun _ ->
    encode_chain (t.index_codecs :> bytestobytes internal_chain) shard_idx
    >>| fun ib ->
    match t.index_location with
    | Start -> ib ^ Buffer.contents buf
    | End -> Buffer.(add_string buf ib; contents buf)

  let rec decode_chain :
    type a b.
    bytestobytes internal_chain ->
    (a, b) Util.array_repr ->
    string ->
    ((a, b) Ndarray.t, [> error]) result
    = fun t repr x ->
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
      t.a2a (ArrayToBytes.decode t.a2b repr' y)

  and decode_index :
    internal_shard_config ->
    int array ->
    string ->
    ((int64, Bigarray.int64_elt) Ndarray.t * string, [> error]) result
    = fun t cps b ->
    let l = index_size t cps in
    let o = String.length b - l in
    let index_bytes, rest =
      match t.index_location with
      | End -> String.sub b o l, String.sub b 0 o
      | Start -> String.sub b 0 l, String.sub b l o
    in
    decode_chain
      (t.index_codecs :> bytestobytes internal_chain)
      {fill_value = Int64.max_int
      ;kind = Bigarray.Int64
      ;shape = Array.append cps [|2|]}
      index_bytes
    >>| fun decoded -> decoded, rest

  and index_size t cps =
    compute_encoded_size (16 * Util.prod cps) t

  and partial_encode :
    internal_shard_config ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ]) result) ->
    partial_setter ->
    int ->
    ('a, 'b) Util.array_repr ->
    (int array * 'a) list ->
    (unit, [> `Store_read of string | error ]) result
    = fun t get_partial set_partial bytesize repr pairs ->
    let open Extensions in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let is = index_size t cps in
    let ibytes, pad =
      match t.index_location with
      | Start -> get_partial [0, Some is], is
      | End -> get_partial [bytesize - is, None], 0 in
    ibytes >>|
    List.hd >>=
    decode_index t cps >>= fun (idx_arr, _) ->
    let tbl = Arraytbl.create @@ List.length pairs in
    RegularGrid.create ~array_shape:repr.shape t.chunk_shape >>= fun grid ->
    List.iter
      (fun (c, v) ->
        let id, co = RegularGrid.index_coord_pair grid c in
        Arraytbl.add tbl id (co, v)) pairs;
    let inner = {repr with shape = t.chunk_shape} in
    let icoords = Array.map (fun v -> [|v; v|]) @@ Ndarray.shape idx_arr in
    icoords.(Array.length t.chunk_shape) <- [|0; 1|];
    ArraySet.fold
      (fun idx acc ->
        acc >>= fun (bsize, shard_idx) ->
        let z = Arraytbl.find_all tbl idx in  
        let p = Bigarray.array1_of_genarray @@ Ndarray.slice_left shard_idx idx in
        let offset = Int64.to_int p.{0} in
        let nb = Int64.to_int p.{1} in
        get_partial [pad + offset, Some nb] >>|
        List.hd >>=
        decode_chain t.codecs inner >>= fun arr ->
        List.iter (fun (c, v) -> Ndarray.set arr c v) z;
        encode_chain t.codecs arr >>| fun s ->
        let nb' = String.length s in
        (* if codec chain doesnt contain compressions so update chunk in-place *)
        if nb' = nb then
          (set_partial [pad + offset, s]; bsize, shard_idx)
        else
          (Array.iteri
            (fun i v -> icoords.(i).(0) <- v; icoords.(i).(1) <- v) idx;
          Ndarray.set_index
            shard_idx icoords Int64.[|of_int bsize; of_int nb'|];
          set_partial ~append:true [bsize, s];
          bsize + nb', shard_idx))
      (ArraySet.of_seq @@ Arraytbl.to_seq_keys tbl) (Ok (bytesize - pad, idx_arr))
    >>= fun (bytesize, shard_idx) ->
    encode_chain (t.index_codecs :> bytestobytes internal_chain) shard_idx
    >>| fun ib ->
    match t.index_location with
    | Start -> set_partial [0, ib]
    | End -> set_partial ~append:true [bytesize, ib]

  and decode :
    t ->
    ('a, 'b) Util.array_repr ->
    string ->
    (('a, 'b) Ndarray.t, [> `Store_read of string | error]) result
    = fun t repr b ->
    let open Extensions in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    decode_index t cps b >>= fun (shard_idx, b') ->
    RegularGrid.create ~array_shape:repr.shape t.chunk_shape >>= fun grid ->
    let slice = Array.make (Array.length repr.shape) @@ Owl_types.R [] in
    let coords = Indexing.coords_of_slice slice repr.shape in
    let tbl = Arraytbl.create @@ Array.length coords in
    Array.iteri
      (fun i y ->
        let k, c = RegularGrid.index_coord_pair grid y in
        Arraytbl.add tbl k (i, c)) coords;
    let inner = {repr with shape = t.chunk_shape} in
    ArraySet.fold
      (fun idx acc ->
        acc >>= fun xs ->
        let pairs = Arraytbl.find_all tbl idx in
        let p = Bigarray.array1_of_genarray @@ Ndarray.slice_left shard_idx idx in
        let c = String.sub b' (Int64.to_int p.{0}) (Int64.to_int p.{1}) in
        decode_chain t.codecs inner c >>| fun arr ->
        List.fold_left
          (fun a (i, c) -> (i, Ndarray.get arr c) :: a) xs pairs)
      (ArraySet.of_seq @@ Arraytbl.to_seq_keys tbl) (Ok [])
    >>| fun pairs ->
    let v =
      Array.of_list @@ snd @@ List.split @@
      List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) pairs in
    Ndarray.of_array inner.kind v repr.shape

  and partial_decode :
    internal_shard_config ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ]) result) ->
    int ->
    ('a, 'b) Util.array_repr ->
    (int * int array) list ->
    ((int * 'a) list, [> `Store_read of string | error ]) result
    = fun t get_partial bsize repr pairs ->
    let open Extensions in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let is = index_size t cps in
    let ibytes, pad =
      match t.index_location with
      | Start -> get_partial [0, Some is], is
      | End -> get_partial [bsize - is, None], 0 in
    ibytes >>|
    List.hd >>=
    decode_index t cps >>= fun (idx_arr, _) ->
    RegularGrid.create ~array_shape:repr.shape t.chunk_shape >>= fun grid ->
    let tbl = Arraytbl.create @@ List.length pairs in
    List.iter
      (fun (i, y) ->
        let id, c = RegularGrid.index_coord_pair grid y in
        Arraytbl.add tbl id (i, c)) pairs;
    let inner = {repr with shape = t.chunk_shape} in
    ArraySet.fold
      (fun idx acc ->
        acc >>= fun (shard_idx, xs) ->
        let z = Arraytbl.find_all tbl idx in
        let p = Bigarray.array1_of_genarray @@ Ndarray.slice_left shard_idx idx in
        get_partial Int64.[pad + to_int p.{0}, Some (to_int p.{1})] >>|
        List.hd >>=
        decode_chain t.codecs inner >>| fun arr ->
        shard_idx, xs @ List.map (fun (i, c) -> (i, Ndarray.get arr c)) z)
      (ArraySet.of_seq @@ Arraytbl.to_seq_keys tbl) (Ok (idx_arr, []))
    >>| snd

  let rec chain_to_yojson chain =
    `List
      (List.map ArrayToArray.to_yojson chain.a2a @
      (ArrayToBytes.to_yojson chain.a2b) ::
      List.map BytesToBytes.to_yojson chain.b2b)

  and to_yojson t =
    let codecs = chain_to_yojson t.codecs in
    let index_codecs =
      chain_to_yojson (t.index_codecs :> bytestobytes internal_chain)
    in
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

  let rec chain_of_yojson :
    Yojson.Safe.t list -> (bytestobytes internal_chain, string) result
    = fun codecs -> 
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

  and of_yojson x =
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
      chain_of_yojson @@ Yojson.Safe.Util.to_list x)
    >>= fun codecs ->
    (match extract "index_codecs" with
    | [] ->
      Error "sharding_indexed must have a index_codecs field"
    | x :: _ ->
      chain_of_yojson @@ Yojson.Safe.Util.to_list x)
    >>= fun ic ->
    (* Ensure index_codecs only contains fixed size bytes-to-bytes codecs. *)
    List.fold_right
      (fun c acc ->
        acc >>= fun l ->
        match c with
        | `Crc32c ->
          Ok (`Crc32c :: l)
        | `Gzip _ ->
          Error "index_codecs must not contain variable-sized codecs.")
      ic.b2b (Ok [])
    >>| fun b2b ->
    {index_codecs = {ic with b2b}; index_location; codecs; chunk_shape}
end
