type arraytoarray = [ `Transpose of int list ]
type deflate_level = L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9
type fixed_bytestobytes = [ `Crc32c ]
type variable_bytestobytes = [ `Gzip of deflate_level | `Zstd of int * bool ]
type bytestobytes = [ fixed_bytestobytes | variable_bytestobytes ]
type loc = Start | End
type endianness = LE | BE
type fixed_arraytobytes = [ `Bytes of endianness ]
type variable_arraytobytes = [ `ShardingIndexed of internal_shard_config ]
and internal_shard_config =
  {chunk_shape : int list
  ;codecs : ([fixed_arraytobytes | `ShardingIndexed of internal_shard_config ], bytestobytes) chain
  ;index_codecs : (fixed_arraytobytes, fixed_bytestobytes) chain
  ;index_location : loc}
and ('a, 'b) chain = {a2a : arraytoarray list; a2b : 'a; b2b : 'b list}
type arraytobytes = [ fixed_arraytobytes | variable_arraytobytes ]
type 'a array_info = {datatype: 'a Ndarray.dtype; shape : int list}

type error =
  [ `Array_to_bytes_invariant
  | `Invalid_transpose_order
  | `Invalid_sharding_chunk_shape
  | `Invalid_codec_ordering
  | `Invalid_zstd_compression_level ]

module ArrayToArray = struct
  module Transpose = struct
    let encoded_size : int -> int = Fun.id
    let encode order x = Ndarray.transpose ~axes:order x
    let encoded_repr ~order (shape : int list) = List.map (fun x -> List.nth shape x) order

    let parse ~order (shape : int list) =
      let o = List.fast_sort Int.compare order in
      let l = List.length o in
      if l = 0 || List.compare_length_with shape l <> 0 || o <> List.init l Fun.id
      then Error `Invalid_transpose_order else Ok ()

    let decode o x =
      let inv_order = Array.(make (List.length o) 0) in
      List.iteri (fun i x -> inv_order.(x) <- i) o;
      Ndarray.transpose ~axes:(Array.to_list inv_order) x

    let to_yojson order : Yojson.Safe.t =
      let o = `List (List.map (fun x -> `Int x) order) in
      `Assoc [("name", `String "transpose"); ("configuration", `Assoc ["order", o])]

    let add_as_int acc = function
      | `Int i when i >= 0 -> Result.map (List.cons i) acc
      | _ -> Error "transpose order values must be non-negative integers."

    let of_yojson chunk_shape x : ([`Transpose of int list], string) Stdlib.result =
      match Yojson.Safe.Util.(member "configuration" x) with
      | `Assoc [("order", `List o)] ->
        begin match List.fold_left add_as_int (Ok []) o with
        | Error _ as e -> e
        | Ok order ->
          match parse ~order chunk_shape with
          | Error `Invalid_transpose_order -> Error "Invalid_transpose_order"
          | Ok () -> Ok (`Transpose order) end
      | _ -> Error "Invalid transpose configuration."
  end

  let parse (t : arraytoarray) shape : (unit, [> error]) result = match t with
    | `Transpose order -> Transpose.parse ~order shape

  let encoded_size input_size (t : arraytoarray) = match t with
    | `Transpose _ -> Transpose.encoded_size input_size

  let encoded_repr shape (t : arraytoarray) = match t with
    | `Transpose order -> Transpose.encoded_repr ~order shape

  let encode x (t : arraytoarray) = match t with
    | `Transpose order -> Transpose.encode order x

  let decode (t : arraytoarray) x = match t with
    | `Transpose order -> Transpose.decode order x

  let to_yojson : arraytoarray -> Yojson.Safe.t = function
    | `Transpose order -> Transpose.to_yojson order

  let of_yojson cs x : (arraytoarray, string) Stdlib.result = match Util.get_name x with
    | "transpose" -> Transpose.of_yojson cs x
    | s -> Error (Printf.sprintf "array->array codec %s not supported" s)
end

module BytesToBytes = struct
  open Bytesrw
  module Gzip = struct
    let to_int = function
      | L0 -> 0 | L1 -> 1 | L2 -> 2 | L3 -> 3 | L4 -> 4
      | L5 -> 5 | L6 -> 6 | L7 -> 7 | L8 -> 8 | L9 -> 9

    let encode l x =
      let r = Bytes.Reader.of_string x in
      Bytes.Reader.to_string (Bytesrw_zlib.Gzip.compress_reads ~level:(to_int l) () r)

    let decode x =
      let r = Bytes.Reader.of_string x in
      Bytes.Reader.to_string (Bytesrw_zlib.Gzip.decompress_reads () r)

    let to_yojson : deflate_level -> Yojson.Safe.t = fun l ->
      `Assoc [("name", `String "gzip"); ("configuration", `Assoc ["level", `Int (to_int l)])]

    let of_int = function
      | 0 -> Ok L0 | 1 -> Ok L1 | 2 -> Ok L2 | 3 -> Ok L3
      | 4 -> Ok L4 | 5 -> Ok L5 | 6 -> Ok L6 | 7 -> Ok L7
      | 8 -> Ok L8 | 9 -> Ok L9 | i ->
        Error (Printf.sprintf "Invalid Gzip level %d" i)

    let of_yojson x = match Yojson.Safe.Util.(member "configuration" x) with
      | `Assoc [("level", `Int i)] -> Result.bind (of_int i) (fun l -> Ok (`Gzip l))
      | _ -> Error "Invalid Gzip configuration."
  end

  module Crc32c = struct
    let encoded_size input_size = input_size + 4
    let decode x = String.sub x 0 (String.length x - 4)
    let to_yojson : Yojson.Safe.t = `Assoc [("name", `String "crc32c")]
    let of_yojson _ = Ok `Crc32c

    let encode x =
      let size = String.length x in
      let buf = Buffer.create size in
      Buffer.add_string buf x;
      let checksum = Checkseum.Crc32c.(default |> unsafe_digest_string x 0 size |> to_int32) in
      Buffer.add_int32_le buf checksum;
      Buffer.contents buf
  end

  module Zstd = struct
    let min_clevel = -131072 and max_clevel = 22
    let parse_clevel l = if l < min_clevel || max_clevel < l then (Error `Invalid_zstd_compression_level) else Ok ()

    let encode clevel checksum x =
      let params = Bytesrw_zstd.Cctx_params.make ~checksum ~clevel () in
      let r = Bytes.Reader.of_string x in
      Bytes.Reader.to_string (Bytesrw_zstd.compress_reads ~params () r)

    let decode x =
      let r = Bytes.Reader.of_string x in
      Bytes.Reader.to_string (Bytesrw_zstd.decompress_reads () r)

    let to_yojson : int -> bool -> Yojson.Safe.t = fun l c ->
      `Assoc [("name", `String "zstd"); ("configuration", `Assoc [("level", `Int l); ("checksum", `Bool c)])]

    let of_yojson x = match Yojson.Safe.Util.(member "configuration" x) with
      | `Assoc [("level", `Int l); ("checksum", `Bool c)] ->
        begin match parse_clevel l with
          | Ok () -> Ok (`Zstd (l, c))
          | Error `Invalid_zstd_compression_level -> Error "Invalid_zstd_level" end
      | _ -> Error "Invalid Zstd configuration."
  end

  let encoded_size input (t : fixed_bytestobytes) = match t with
    | `Crc32c -> Crc32c.encoded_size input

  let parse : bytestobytes -> (unit, [> error]) result = function
    | `Zstd (l, _) -> Zstd.parse_clevel l
    | (`Gzip _ | `Crc32c) -> Ok ()

  let encode x (t : bytestobytes) = match t with
    | `Gzip l -> Gzip.encode l x
    | `Crc32c -> Crc32c.encode x
    | `Zstd (l, c) -> Zstd.encode l c x

  let decode (t : bytestobytes) x = match t with
    | `Gzip _ -> Gzip.decode x
    | `Crc32c -> Crc32c.decode x
    | `Zstd _ -> Zstd.decode x

  let to_yojson : bytestobytes -> Yojson.Safe.t = function
    | `Gzip l -> Gzip.to_yojson l
    | `Crc32c -> Crc32c.to_yojson 
    | `Zstd (l, c) -> Zstd.to_yojson l c

  let of_yojson x : (bytestobytes, string) Stdlib.result = match Util.get_name x with
    | "gzip" -> Gzip.of_yojson x
    | "crc32c" -> Crc32c.of_yojson x
    | "zstd" -> Zstd.of_yojson x
    | s -> Error (Printf.sprintf "codec %s is not supported." s)
end

module CoordMap = Util.CoordMap
module RegularGrid = Extensions.RegularGrid

module rec ArrayToBytes : sig
  module Make (IO : Types.IO) (Store : Types.Store with type 'a io = 'a IO.t) : sig
    type t = internal_shard_config
    val partial_encode :
      fill_value:'a ->
      Store.t ->
      Types.key ->
      t ->
      'a array_info ->
      (Types.chunk_coord * 'a) list ->
      (unit, [> `Zarr of Store.error ]) result IO.t
    val partial_decode :
      fill_value:'a ->
      Store.t ->
      Types.key ->
      t ->
      'a array_info ->
      (int * Types.chunk_coord) list ->
      ((int * 'a) list, [> `Zarr of Store.error ]) result IO.t
  end
  val parse : arraytobytes -> int list -> (unit, [> error]) result
  val encoded_size : int -> fixed_arraytobytes -> int
  val encode : arraytobytes -> 'a Ndarray.t -> string
  val decode : arraytobytes -> 'a array_info -> string -> 'a Ndarray.t
  val of_yojson : int list -> Yojson.Safe.t -> (arraytobytes, string) Stdlib.result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end = struct

  module Make (IO : Types.IO) (Store : Types.Store with type 'a io = 'a IO.t) = struct
    type t = ShardingIndexed.t
    open IO.Syntax

    let add_binding ~shard_grid acc (shard_coord, element) =
      let innerchunk_index, coord_within_innerchunk = RegularGrid.index_coord_pair shard_grid shard_coord in
      CoordMap.add_to_list innerchunk_index (coord_within_innerchunk, element) acc

    (* specialized function for partially writing multiple inner chunks to an empty shard of a designated array using the sharding indexed codec.*)
    let partial_encode_empty_shard fill_value store shard_key shard_params repr coord_elem_pairs =
      let update_innerchunk ~shard_params ~index_array ~fill_value data_type innerchunk_index innerchunk_coord_elem_pairs (offset, acc) =
        let arr = Ndarray.create data_type shard_params.chunk_shape fill_value in
        List.iter (fun (coords, element) -> Ndarray.set arr coords element) innerchunk_coord_elem_pairs;
        let innerchunk_data = ShardingIndexed.encode_innerchunk shard_params.codecs arr in
        let nbytes = String.length innerchunk_data in
        Ndarray.set index_array (innerchunk_index @ [0]) (Stdint.Uint64.of_int offset);
        Ndarray.set index_array (innerchunk_index @ [1]) (Stdint.Uint64.of_int nbytes);
        offset + nbytes, (offset, innerchunk_data) :: acc
      in
      let chunk_per_shard = List.map2 (/) repr.shape shard_params.chunk_shape in
      let initial_offset = match shard_params.index_location with
        | Start -> ShardingIndexed.index_size shard_params.index_codecs chunk_per_shard
        | End -> 0
      in
      (* simulate the inner chunks of a shard using a regular grid of specific shape.*)
      let shard_grid = Result.get_ok (RegularGrid.create ~array_shape:repr.shape shard_params.chunk_shape) in
      (* build a finite map with its keys being an inner chunk's index and values
         being a list of (coord-within-inner-chunk, element) pairs such that
         element is set for the coordinate coord-within-inner-chunk of the inner
         chunk represented by the associated key/index.*)
      let index_array = Ndarray.create Uint64 (chunk_per_shard @ [2]) Stdint.Uint64.max_int in
      let m = List.fold_left (add_binding ~shard_grid) CoordMap.empty coord_elem_pairs in
      let f = update_innerchunk ~shard_params ~index_array ~fill_value repr.datatype in
      let shardsize, offset_data_pairs = CoordMap.fold f m (initial_offset, []) in
      let indexbytes = ShardingIndexed.encode_index_chain shard_params.index_codecs index_array in
      (* write all resultant (offset, bytes) pairs into the bytes of the new shard
         taking note to append/prepend the bytes of the shard's index array.*)
      match shard_params.index_location with
      | Start ->
        let offset_data_pairs' = (0, indexbytes) :: List.rev offset_data_pairs in
        Store.set_partial_values store shard_key offset_data_pairs'
      | End ->
        let offset_data_pairs' = List.rev ((shardsize, indexbytes) :: offset_data_pairs) in
        Store.set_partial_values store shard_key offset_data_pairs'

    (* function to partially write new elements to one or more inner chunks of
       an existing shard using the sharding indexed codec. *)
    let partial_encode ~fill_value store shard_key shard_params repr coord_elem_pairs =
      let choose ~index_array innerchunk_index element (l, r) =
        let offset_coords = innerchunk_index @ [0] and nbytes_coords = innerchunk_index @ [1] in
        match Ndarray.(get index_array offset_coords, get index_array nbytes_coords) with
        | offset, nbytes when Stdint.Uint64.(max_int = offset && max_int = nbytes) ->
          (offset_coords, nbytes_coords, element) :: l, r
        | offset, nbytes ->
          l, (Stdint.Uint64.to_int offset, Stdint.Uint64.to_int nbytes, offset_coords, nbytes_coords, element) :: r
      in
      let update_nonempty_innerchunk ~repr' ~index_array codec_chain (acc, l, r) data (offset, nbytes, offset_coords, nbytes_coords, innerchunk_coord_elem_pairs) =
        let arr = ShardingIndexed.decode_innerchunk codec_chain repr' data in
        List.iter (fun (coords, element) -> Ndarray.set arr coords element) innerchunk_coord_elem_pairs;
        let data' = ShardingIndexed.encode_innerchunk codec_chain arr in
        let nbytes' = String.length data' in
        if nbytes' = nbytes then acc, (offset, data') :: l, r else begin
          Ndarray.set index_array offset_coords (Stdint.Uint64.of_int acc);
          Ndarray.set index_array nbytes_coords (Stdint.Uint64.of_int nbytes');
          acc + nbytes', l, (acc, data') :: r
        end
      in
      let update_empty_innerchunk ~shard_params ~index_array ~fill_value data_type (offset, acc) (offset_coords, nbytes_coords, innerchunk_coord_elem_pairs) =
        let arr = Ndarray.create data_type shard_params.chunk_shape fill_value in 
        List.iter (fun (coords, element) -> Ndarray.set arr coords element) innerchunk_coord_elem_pairs;
        let innerchunk_data = ShardingIndexed.encode_innerchunk shard_params.codecs arr in
        let nbytes = String.length innerchunk_data in
        Ndarray.set index_array offset_coords (Stdint.Uint64.of_int offset);
        Ndarray.set index_array nbytes_coords (Stdint.Uint64.of_int nbytes);
        offset + nbytes, (offset, innerchunk_data) :: acc
      in
      (* begin *)
      let* shard_size = Store.size store shard_key in
      if shard_size = 0 then partial_encode_empty_shard fill_value store shard_key shard_params repr coord_elem_pairs else
      let chunks_per_shard = List.map2 (/) repr.shape shard_params.chunk_shape in
      let index_size = ShardingIndexed.index_size shard_params.index_codecs chunks_per_shard in
      let* index_data = match shard_params.index_location with
        | Start -> IO.map List.hd (Store.get_partial_values store shard_key [0, Some index_size])
        | End -> IO.map List.hd (Store.get_partial_values store shard_key [shard_size - index_size, None])
      in
      let index_array = fst @@ ShardingIndexed.decode_index shard_params chunks_per_shard index_data in
      (* Using Result.get_ok here is safe since RegularGrid.create is guaranteed to be called with correct arguments *)
      let shard_grid = Result.get_ok (RegularGrid.create ~array_shape:repr.shape shard_params.chunk_shape) in
      let m = List.fold_left (add_binding ~shard_grid) CoordMap.empty coord_elem_pairs in
      (* split the finite map m into key-value pairs representing empty inner chunks
         and those that don't (using the fact that empty inner chunks have index
         array values equal to 2^64 - 1; then process these seperately.*)
      let empty, nonempty = CoordMap.fold (choose ~index_array) m ([], []) in
      let ranges = List.map (fun (offset, nbytes, _, _, _) -> offset, Some nbytes) nonempty in
      let* innerchunks = Store.get_partial_values store shard_key ranges in
      let repr' = {repr with shape = shard_params.chunk_shape} in
      (* fold over the nonempty index coordinates and finite map to obtain
         (offset, bytes) pairs to write in-place and those to append at the
         end of the shard. bytes to write in-place are determined by comparing
         encoded size vs the corresponding nbytes[i] value already contained in
         the shard's index array.*)
      let shard_size', indexed_innerchunks, indexed_innerchunks' =
        ListLabels.fold_left2
          ~f:(update_nonempty_innerchunk ~repr' ~index_array shard_params.codecs)
          ~init:(shard_size, [], [])
          innerchunks
          nonempty
      in
      let* () = match indexed_innerchunks with
        | [] -> IO.return_unit
        | rs -> Store.set_partial_values store shard_key rs
      in
      let* () = match indexed_innerchunks' with
        | [] -> IO.return_unit
        | rs -> Store.set_partial_values store shard_key ~append:true (List.rev rs)
      in
      (* new values that need to be written to previously empty inner chunks will
         be appended at the end of the shard and the corresponding index array's
         offset and number-of-bytes values updated accordingly.*)
      let shard_size'', indexed_innerchunks'' =
        ListLabels.fold_left
          ~f:(update_empty_innerchunk ~shard_params ~index_array ~fill_value repr.datatype)
          ~init:(shard_size', [])
          empty
      in
      let* () = match indexed_innerchunks'' with
        | [] -> IO.return_unit
        | rs -> Store.set_partial_values store shard_key ~append:true (List.rev rs)
    in
    let indexbytes = ShardingIndexed.encode_index_chain shard_params.index_codecs index_array in
    match shard_params.index_location with
    | Start -> Store.set_partial_values store shard_key [(0, indexbytes)]
    | End -> Store.set_partial_values store shard_key ~append:true [(shard_size'', indexbytes)]
      (* end *)

    (* function to partially read values off of a non-empty shard previously
       encoded using the sharding indexed codec. *) 
    let partial_decode ~fill_value store chunk_key shard_params repr (indexed_shard_coords : (int * Types.chunk_coord) list) =
      let add_indexed_innerchunk_coord ~shard_grid acc ((i, shard_coord) : int * int list) =
        let innerchunk_index, coord_within_innerchunk = RegularGrid.index_coord_pair shard_grid shard_coord in
        CoordMap.add_to_list innerchunk_index (i, coord_within_innerchunk) acc
      in
      let choose ~index_array innerchunk_index (indexed_innerchunk_coords : (int * int list) list) (l, r) =
        match Ndarray.(get index_array (innerchunk_index @ [0]), get index_array (innerchunk_index @ [1])) with
        | offset, nbytes when Stdint.Uint64.(max_int = offset && max_int = nbytes) ->
          l @ fst (List.split indexed_innerchunk_coords), r
        | offset, nbytes ->
          l, ((Stdint.Uint64.to_int offset, Some (Stdint.Uint64.to_int nbytes)), indexed_innerchunk_coords) :: r
      in
      let indexed_innerchunk_element repr' acc data (indexed_innerchunk_coords : (int * int list) list) =
        let arr = ShardingIndexed.decode_innerchunk shard_params.codecs repr' data in
        acc @ List.map (fun (i, coords) -> i, Ndarray.get arr coords) indexed_innerchunk_coords
      in
      let chunks_per_shard = List.map2 (/) repr.shape shard_params.chunk_shape in
      let index_size = ShardingIndexed.index_size shard_params.index_codecs chunks_per_shard in
      let* shard_size = Store.size store chunk_key in
      let* shard_data = match shard_params.index_location with
        | Start -> IO.map List.hd (Store.get_partial_values store chunk_key [(0, Some index_size)])
        | End -> IO.map List.hd (Store.get_partial_values store chunk_key [(shard_size - index_size, None)])
      in
      let index_array = fst @@ ShardingIndexed.decode_index shard_params chunks_per_shard shard_data in
      let shard_grid = Result.get_ok (RegularGrid.create ~array_shape:repr.shape shard_params.chunk_shape) in
      let m = List.fold_left (add_indexed_innerchunk_coord ~shard_grid) CoordMap.empty indexed_shard_coords in
      let empty, nonempty = CoordMap.fold (choose ~index_array) m ([], []) in
      let ranges, indexed_innerchunk_coords = List.split nonempty in
      let+ innerchunks = Store.get_partial_values store chunk_key ranges in
      let repr' = {repr with shape = shard_params.chunk_shape} in
      let res1 = List.fold_left2 (indexed_innerchunk_element repr') [] innerchunks indexed_innerchunk_coords in
      let res2 = List.map (fun i -> i, fill_value) empty in  
      res1 @ res2  (* indexed chunk coord data *)
  end

  let parse (t : arraytobytes) shape : (unit, [> error]) result = match t with
    | `Bytes _ -> Ok ()
    | `ShardingIndexed c -> ShardingIndexed.parse c shape

  let encoded_size input_size (t : fixed_arraytobytes) = match t with
    | `Bytes _ -> Bytes'.encoded_size input_size

  let encode (t : arraytobytes) x = match t with
    | `Bytes endian -> Bytes'.encode x endian
    | `ShardingIndexed c -> ShardingIndexed.encode c x

  let decode (t : arraytobytes) repr b = match t with
    | `Bytes endian -> Bytes'.decode b repr endian
    | `ShardingIndexed c -> ShardingIndexed.decode c repr b

  let to_yojson : arraytobytes -> Yojson.Safe.t = function
    | `Bytes endian -> Bytes'.to_yojson endian
    | `ShardingIndexed c -> ShardingIndexed.to_yojson c

  let of_yojson shape x : (arraytobytes, string) Stdlib.result =
    match Util.get_name x with
    | "bytes" -> Result.map (fun e -> `Bytes e) (Bytes'.of_yojson x)
    | "sharding_indexed" -> Result.map (fun c -> `ShardingIndexed c) (ShardingIndexed.of_yojson shape x)
    | _ -> Error ("array->bytes codec not supported: ")
end

and Bytes' : sig
  val encoded_size : int -> int
  val encode : 'a Ndarray.t -> endianness -> string
  val decode : string -> 'a array_info -> endianness -> 'a Ndarray.t
  val of_yojson : Yojson.Safe.t -> (endianness, string) Stdlib.result
  val to_yojson : endianness -> Yojson.Safe.t
end = struct
  let encoded_size : int -> int = Fun.id 

  let endian_module = function
    | LE -> (module Ebuffer.Little : Ebuffer.S)
    | BE -> (module Ebuffer.Big : Ebuffer.S)

  let encode (type a) (x : a Ndarray.t) e : string =
    let open (val endian_module e) in
    let buf = Bytes.create (Ndarray.byte_size x) in
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

  let decode (type a) (str : string) (decoded : a array_info) e : a Ndarray.t =
    let open (val endian_module e) in
    let k, shape = decoded.datatype, decoded.shape in
    let buf = Bytes.unsafe_of_string str in
    match k, Ndarray.dtype_size k with
    | Char, _ -> Ndarray.init k shape (get_char buf)
    | Bool, _ -> Ndarray.init k shape (get_bool buf)
    | Uint8, _ -> Ndarray.init k shape (get_int8 buf)
    | Int8, _ -> Ndarray.init k shape (get_uint8 buf)
    | Int16, s -> Ndarray.init k shape (fun i -> get_int16 buf (i*s))
    | Uint16, s -> Ndarray.init k shape (fun i -> get_uint16 buf (i*s))
    | Int32, s -> Ndarray.init k shape (fun i -> get_int32 buf (i*s))
    | Int64, s -> Ndarray.init k shape (fun i -> get_int64 buf (i*s))
    | Uint64, s -> Ndarray.init k shape (fun i -> get_uint64 buf (i*s))
    | Float32, s -> Ndarray.init k shape (fun i -> get_float32 buf (i*s))
    | Float64, s -> Ndarray.init k shape (fun i -> get_float64 buf (i*s))
    | Complex32, s -> Ndarray.init k shape (fun i -> get_complex32 buf (i*s))
    | Complex64, s -> Ndarray.init k shape (fun i -> get_complex64 buf (i*s))
    | Int, s -> Ndarray.init k shape (fun i -> get_int buf (i*s))
    | Nativeint, s -> Ndarray.init k shape (fun i -> get_nativeint buf (i*s))

  let to_yojson e : Yojson.Safe.t =
    let endian = match e with
      | LE -> "little"
      | BE -> "big"
    in
    `Assoc [("name", `String "bytes"); ("configuration", `Assoc ["endian", `String endian])]

  let of_yojson x = match Yojson.Safe.Util.(member "configuration" x) with
    | `Assoc [("endian", `String e)] ->
      begin match e with
        | "little" -> Ok LE
        | "big" -> Ok BE
        | s -> Error (Printf.sprintf "Unsupported endianness: %s" s)
      end
    | _ -> Error "Invalid bytes codec configuration."
end

and ShardingIndexed : sig
  type t = internal_shard_config
  val parse : t -> int list -> (unit, [> error]) result
  val encode : t -> 'a Ndarray.t -> string
  val decode : t -> 'a array_info -> string -> 'a Ndarray.t
  val of_yojson : int list -> Yojson.Safe.t -> (t, string) Stdlib.result
  val to_yojson : t -> Yojson.Safe.t
  val encode_innerchunk : (arraytobytes, bytestobytes) chain -> 'a Ndarray.t -> string
  val decode_innerchunk : (arraytobytes, bytestobytes) chain -> 'a array_info -> string -> 'a Ndarray.t
  val decode_index : t -> int list -> string -> Stdint.uint64 Ndarray.t * string
  val index_size : (fixed_arraytobytes, fixed_bytestobytes) chain -> int list -> int
  val encode_index_chain : (fixed_arraytobytes, fixed_bytestobytes) chain -> Stdint.uint64 Ndarray.t -> string
end = struct
  module Indexing = Ndarray.Indexing
  type t = internal_shard_config  

  let parse_chain (shape : int list) (chain : (arraytobytes, bytestobytes) chain) =
    let shape' = match chain.a2a with
      | [] -> Ok shape
      | x :: _ as xs ->
        Result.map
          (fun () -> List.fold_left ArrayToArray.encoded_repr shape xs)
          (ArrayToArray.parse x shape)
    in
    Result.bind shape' (ArrayToBytes.parse chain.a2b)

  let parse t shape = match t.chunk_shape with
    | c when List.(length shape <> length c) -> Error `Invalid_sharding_chunk_shape
    | c when not @@ List.for_all2 (fun x y -> (x mod y) = 0) shape c -> Error `Invalid_sharding_chunk_shape
    | _ ->
      Result.bind (parse_chain shape t.codecs) @@ fun () ->
      parse_chain (shape @ [2]) (t.index_codecs :> (arraytobytes, bytestobytes) chain)

  let encoded_size init chain =
    let a2a_size = List.fold_left ArrayToArray.encoded_size init chain.a2a in
    let a2b_size = ArrayToBytes.encoded_size a2a_size chain.a2b in
    List.fold_left BytesToBytes.encoded_size a2b_size chain.b2b
  
  let encode_innerchunk chain x =
    let a = List.fold_left ArrayToArray.encode x chain.a2a in
    let b = ArrayToBytes.encode chain.a2b a in
    List.fold_left BytesToBytes.encode b chain.b2b

  let encode_index_chain (t : (fixed_arraytobytes, fixed_bytestobytes) chain) x =
    let y = match t.a2a with
      | [] -> x
      | `Transpose o :: _ -> Ndarray.transpose ~axes:o x
    in
    let z = match t.a2b with
      | `Bytes e -> Bytes'.encode y e
    in
    List.fold_left BytesToBytes.encode z (t.b2b :> bytestobytes list)

  let encode (type a) (t : t) (x : a Ndarray.t) =
    let add_coord ~grid ~arr coord acc =
      let k, c = RegularGrid.index_coord_pair grid coord in
      CoordMap.add_to_list k (c, Ndarray.get arr coord) acc
    in
    let update_inner_chunk ~t ~shard_idx ~datatype i pairs (ofs, xs) =
      let v = Array.of_list (List.map snd pairs) in
      let x' = Ndarray.of_array datatype t.chunk_shape v in
      let b = encode_innerchunk t.codecs x' in
      let nb = Stdint.Uint64.of_int (String.length b) in
      Ndarray.set shard_idx (i @ [0]) ofs;
      Ndarray.set shard_idx (i @ [1]) nb;
      Stdint.Uint64.(ofs + nb), b :: xs
    in
    let shard_shape = Ndarray.shape x in
    let cps = List.map2 (/) shard_shape t.chunk_shape in
    let shard_idx = Ndarray.create Uint64 (cps @ [2]) Stdint.Uint64.max_int in
    let grid = Result.get_ok (RegularGrid.create ~array_shape:shard_shape t.chunk_shape) in
    let datatype = Ndarray.data_type x in
    let slice = Result.get_ok (Indexing.create [] shard_shape) in
    let m = List.fold_right (add_coord ~grid ~arr:x) (Indexing.coords_of_slice slice) CoordMap.empty in
    let _, xs = CoordMap.fold (update_inner_chunk ~t ~shard_idx ~datatype) m (Stdint.Uint64.zero, []) in
    let idx_bytes = encode_index_chain t.index_codecs shard_idx in
    match t.index_location with
    | Start -> String.concat String.empty (idx_bytes :: List.rev xs)
    | End -> String.concat String.empty (List.rev (idx_bytes :: xs))

  let decode_innerchunk t repr x =
    let shape = List.fold_left ArrayToArray.encoded_repr repr.shape t.a2a in
    let b2b = List.fold_right BytesToBytes.decode t.b2b x in
    let a2b = ArrayToBytes.decode t.a2b {repr with shape} b2b in
    List.fold_right ArrayToArray.decode t.a2a a2b

  let decode_index_chain (t: (fixed_arraytobytes, fixed_bytestobytes) chain) shape x =
    let shape' = List.fold_left ArrayToArray.encoded_repr shape t.a2a in
    let y = List.fold_right BytesToBytes.decode (t.b2b :> bytestobytes list) x in
    let arr = match t.a2b with
      | `Bytes e -> Bytes'.decode y {shape=shape'; datatype=Uint64} e in
    match t.a2a with
    | [] -> arr
    | `Transpose o :: _ -> ArrayToArray.Transpose.decode o arr

  let index_size index_chain chunks_per_shard =
    encoded_size (16 * List.fold_left Int.mul 1 chunks_per_shard) index_chain

  let decode_index t chunks_per_shard shard_data =
    let l = index_size t.index_codecs chunks_per_shard in
    let o = String.length shard_data - l in
    let index_data, chunk_data = match t.index_location with
      | End -> String.sub shard_data o l, String.sub shard_data 0 o
      | Start -> String.sub shard_data 0 l, String.sub shard_data l o
    in
    decode_index_chain t.index_codecs (chunks_per_shard @ [2]) index_data, chunk_data

  let decode (type a) (t : t) (repr : a array_info) (b : string) =
    let add_indexed_coord ~grid acc i coord =
      let k, c = RegularGrid.index_coord_pair grid coord in
      CoordMap.add_to_list k (i, c) acc
    in
    let read_inner_chunk ~t ~idx_arr ~inner_repr ~chunk_bytes key value acc =
      let ofs = Stdint.Uint64.to_int (Ndarray.get idx_arr (key @ [0])) in
      let nb = Stdint.Uint64.to_int (Ndarray.get idx_arr (key @ [1])) in
      let arr = decode_innerchunk t.codecs inner_repr (String.sub chunk_bytes ofs nb) in
      acc @ List.map (fun ((i, c) : int * int list) -> i, Ndarray.get arr c) value
    in
    let chunks_per_shard = List.map2 (/) repr.shape t.chunk_shape in
    let idx_arr, chunk_bytes = decode_index t chunks_per_shard b in
    let grid = Result.get_ok (RegularGrid.create ~array_shape:repr.shape t.chunk_shape) in
    let slice = Result.get_ok (Indexing.create [] repr.shape) in
    let coords = Indexing.coords_of_slice slice in
    let m = List.fold_left2 (add_indexed_coord ~grid) CoordMap.empty List.(init (length coords) Fun.id) coords in
    let inner_repr = {repr with shape = t.chunk_shape} in
    let pairs = CoordMap.fold (read_inner_chunk ~t ~idx_arr ~inner_repr ~chunk_bytes) m [] in
    let sorted_pairs = List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) pairs in
    let vs = List.map snd sorted_pairs in
    Ndarray.of_array inner_repr.datatype repr.shape (Array.of_list vs)

  let chain_to_yojson : (arraytobytes, bytestobytes) chain -> Yojson.Safe.t = fun chain ->
    let a2a = List.map ArrayToArray.to_yojson chain.a2a in
    let a2b = ArrayToBytes.to_yojson chain.a2b in
    let b2b = List.map BytesToBytes.to_yojson chain.b2b in
    `List (a2a @ (a2b :: b2b))

  let to_yojson : t -> Yojson.Safe.t = fun t ->
    let index_codecs = chain_to_yojson (t.index_codecs :> (arraytobytes, bytestobytes) chain) in
    let index_location = match t.index_location with
      | End -> `String "end"
      | Start -> `String "start"
    in
    `Assoc
    [("name", `String "sharding_indexed");
     ("configuration", `Assoc
      [("chunk_shape", `List (List.map (fun x -> `Int x) t.chunk_shape));
       ("index_location", index_location);
       ("index_codecs", index_codecs);
       ("codecs", chain_to_yojson t.codecs)])]

  let chain_of_yojson (chunk_shape : int list) codecs =
    let split ~f codec (l, r) = Result.fold ~ok:(fun v -> v :: l, r) ~error:(fun _ -> l, codec :: r) (f codec) in
    let partition f encoded = List.fold_right (split ~f) encoded ([], []) in
    match codecs with
    | [] -> Error "No codec chain specified for sharding_indexed."
    | y -> match partition (ArrayToBytes.of_yojson chunk_shape) y with
      | ([], _ | _::_::_, _) -> Error "Must be exactly one array->bytes codec."
      | a2b :: [], xs ->
        let a2a, rest = partition (ArrayToArray.of_yojson chunk_shape) xs in
        match partition BytesToBytes.of_yojson rest with
        | b2b, [] -> Ok {a2a; a2b; b2b}
        | _, x :: _ -> Error (Printf.sprintf "%s codec is unsupported or has invalid configuration." (Util.get_name x))

  let of_yojson (shard_shape : int list) x =
    let open Util.Result_syntax in
    let extract ~assoc name = Yojson.Safe.Util.filter_map (fun (n, v) -> if n = name then Some v else None) assoc in
    let add_as_int a acc = Result.bind acc @@ fun k -> match a with
      | `Int i when i > 0 -> Ok (i :: k)
      | _ -> Error "chunk_shape must only contain positive integers."
    in
    let add_fixed_size_codec ~error_msg c acc = Result.bind acc @@ fun l -> match c with
      | `Crc32c -> Ok (`Crc32c :: l)
      | `Gzip _ | `Zstd _ -> Error error_msg
    in
    let assoc = Yojson.Safe.Util.(member "configuration" x |> to_assoc) in
    let* index_location = match extract ~assoc "index_location" with
      | `String "end" :: [] -> Ok End
      | `String "start" :: [] -> Ok Start
      | [] -> Error "sharding_indexed must have a index_location field"
      | _ -> Error "index_location must only be 'end' or 'start'"
    and* chunk_shape = match extract ~assoc "chunk_shape" with
      | [] -> Error "sharding_indexed must contain a chunk_shape field"
      | x :: _ -> List.fold_right add_as_int (Yojson.Safe.Util.to_list x) (Ok [])
    in
    let* codecs = match extract ~assoc "codecs" with
      | [] -> Error "sharding_indexed must have a codecs field"
      | x :: _ -> chain_of_yojson chunk_shape (Yojson.Safe.Util.to_list x)
    and* ic = match extract ~assoc "index_codecs" with
      | [] -> Error "sharding_indexed must have a index_codecs field"
      | x :: _ ->
        let cps = List.map2 (/) shard_shape chunk_shape in
        chain_of_yojson (cps @ [2]) (Yojson.Safe.Util.to_list x)
    in
    (* Ensure index_codecs only contains fixed size
       array->bytes and bytes->bytes codecs. *)
    let error_msg = "index_codecs must not contain variable-sized codecs." in
    let* b2b = List.fold_right (add_fixed_size_codec ~error_msg) ic.b2b (Ok []) in
    let+ a2b = match ic.a2b with
      | `Bytes e -> Ok (`Bytes e)
      | `ShardingIndexed _ -> Error error_msg
    in
    {index_codecs = {ic with a2b; b2b}; index_location; codecs; chunk_shape}
end

type variable_array_tobytes = [ `ShardingIndexed of shard_config ]
and codec = [ arraytoarray | fixed_arraytobytes | `ShardingIndexed of shard_config | bytestobytes ]
and index_codec = [ arraytoarray | fixed_arraytobytes | fixed_bytestobytes ]
and shard_config =
  {chunk_shape : int list
  ;codecs : codec list
  ;index_codecs : index_codec list
  ;index_location : loc}

module Chain = struct
  type t = (arraytobytes, bytestobytes) chain

  let rec create shape chain =
    let open Util.Result_syntax in
    let* a2a, encoded_shape, rest = extract_arraytoarray [] shape chain in
    let* a2b, other = extract_arraytobytes encoded_shape rest in
    let* b2b, rest = extract_bytestobytes [] other in
    match rest with
    | _::_ -> Error `Invalid_codec_ordering
    | [] -> Ok {a2a; a2b; b2b}

  and extract_arraytoarray l shape r = match r with
    | (#arraytoarray as x) :: xs -> extract_arraytoarray (l @ [x]) shape xs
    | _ -> match l with
      | [] as e -> Ok (e, shape, r)
      | x :: _ -> Result.map (fun () -> l, List.fold_left ArrayToArray.encoded_repr shape l, r) (ArrayToArray.parse x shape)

  and extract_bytestobytes l r = match r with
    | (#bytestobytes as x) :: xs -> extract_bytestobytes (l @ [x]) xs
    | _ -> match l with
      | [] as e -> Ok (e, r)
      | _ ->
        let is_ok = List.fold_left (fun acc b -> Result.bind acc (fun () -> BytesToBytes.parse b)) (Ok ()) l in
        Result.map (fun () -> l, r) is_ok

  and extract_arraytobytes shape = function
    | (#fixed_arraytobytes as x) :: xs -> Result.map (fun () -> x, xs) (ArrayToBytes.parse x shape)
    | (#variable_array_tobytes as x) :: xs ->
      let open Util.Result_syntax in
      begin match x with
      | `ShardingIndexed cfg ->
        let* codecs = create shape cfg.codecs
        and* index_codecs = create (shape @ [2]) (cfg.index_codecs :> codec list) in
        (* coerse to a fixed codec chain list type *)
        let* a2b = match index_codecs.a2b with
          | #fixed_arraytobytes as c -> Ok c
          | _ -> Error `Array_to_bytes_invariant 
        in
        let pred = function #fixed_bytestobytes as c -> Some c | _ -> None in
        let cfg' : internal_shard_config = {
          index_codecs = {index_codecs with a2b; b2b = List.filter_map pred index_codecs.b2b};
          index_location = cfg.index_location;
          chunk_shape = cfg.chunk_shape;
          codecs;
        } in
        Result.map (fun () -> `ShardingIndexed cfg', xs) (ArrayToBytes.parse (`ShardingIndexed cfg') shape)
      end
    | _ -> Error `Array_to_bytes_invariant
 
  let encode t x =
    let a = List.fold_left ArrayToArray.encode x t.a2a in
    let b = ArrayToBytes.encode t.a2b a in
    List.fold_left BytesToBytes.encode b t.b2b

  let decode t repr x =
    let shape = List.fold_left ArrayToArray.encoded_repr repr.shape t.a2a in
    let b = List.fold_right BytesToBytes.decode t.b2b x in
    let a = ArrayToBytes.decode t.a2b {repr with shape} b in
    List.fold_right ArrayToArray.decode t.a2a a

  let ( = ) x y =
    x.a2a = y.a2a && x.a2b = y.a2b && x.b2b = y.b2b

  let to_yojson : t -> Yojson.Safe.t = fun t ->
    let a2a = List.map ArrayToArray.to_yojson t.a2a in
    let a2b = ArrayToBytes.to_yojson t.a2b in
    let b2b = List.map BytesToBytes.to_yojson t.b2b in
    `List (a2a @ (a2b :: b2b))

  let of_yojson chunk_shape (x : Yojson.Safe.t) =
    let split ~f codec (l, r) = Result.fold ~ok:(fun v -> v :: l, r) ~error:(fun _ -> l, codec :: r) (f codec) in
    let partition f encoded = List.fold_right (split ~f) encoded ([], []) in
    match x with
    | `List codecs ->
      begin match partition (ArrayToBytes.of_yojson chunk_shape) codecs with
      | ([], _ | _::_::_, _) -> Error "Must be exactly one array->bytes codec."
      | [a2b], rest ->
        let a2a, rest = partition (ArrayToArray.of_yojson chunk_shape) rest in
        let b2b, rest = partition BytesToBytes.of_yojson rest in
        match rest with
        | [] -> Ok {a2a; a2b; b2b}
        | x :: _ -> Error (Printf.sprintf "%s codec is unsupported or has invalid configuration." (Util.get_name x))
      end
    | `Null -> Error "array metadata must contain a codecs field."
    | _ -> Error "codecs field must be a list of objects."
end

module Make (IO : Types.IO) (Store : Types.Store with type 'a io = 'a IO.t) = struct
  module M = ArrayToBytes.Make(IO)(Store)

  let is_just_sharding : Chain.t -> bool = function
    | {a2a = []; a2b = `ShardingIndexed _; b2b = []} -> true
    | _ -> false

  let partial_encode ~fill_value store chunk_key t repr pairs = match t.a2b with
    | `ShardingIndexed config -> M.partial_encode ~fill_value store chunk_key config repr pairs
    | `Bytes _ -> failwith "bytes codec does not support partial encoding."  (* path that's never reached *)

  let partial_decode ~fill_value store chunk_key t repr pairs = match t.a2b with
    | `ShardingIndexed config -> M.partial_decode ~fill_value store chunk_key config repr pairs
    | `Bytes _ -> failwith "bytes codec does not support partial decoding."  (* path that's never reached *)
end
