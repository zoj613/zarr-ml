exception Array_to_bytes_invariant
exception Invalid_transpose_order
exception Invalid_sharding_chunk_shape
exception Invalid_codec_ordering
exception Invalid_zstd_level

type arraytoarray = [ `Transpose of int array ]
type compression_level = L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9
type fixed_bytestobytes = [ `Crc32c ]
type variable_bytestobytes = [ `Gzip of compression_level | `Zstd of int * bool ]
type bytestobytes = [ fixed_bytestobytes | variable_bytestobytes ]
type loc = Start | End
type endianness = LE | BE
type fixed_arraytobytes = [ `Bytes of endianness ]
type variable_arraytobytes = [ `ShardingIndexed of internal_shard_config ]
and internal_shard_config =
  {chunk_shape : int array
  ;codecs : ([fixed_arraytobytes | `ShardingIndexed of internal_shard_config ], bytestobytes) chain
  ;index_codecs : (fixed_arraytobytes, fixed_bytestobytes) chain
  ;index_location : loc}
and ('a, 'b) chain = {a2a : arraytoarray list; a2b : 'a; b2b : 'b list}
type arraytobytes = [ fixed_arraytobytes | variable_arraytobytes ]
type 'a array_repr = {kind : 'a Ndarray.dtype; shape : int array}

module ArrayToArray = struct
  module Transpose = struct
    let encoded_size : int -> int = Fun.id
    let encode order x = Ndarray.transpose ~axes:order x

    let encoded_repr : order:int array -> int array -> int array = fun ~order shape ->
      Array.map (fun x -> shape.(x)) order

    let parse : order:int array -> int array -> unit = fun ~order shape ->
      let o = Array.copy order in
      Array.fast_sort Int.compare o;
      let l = Array.length order in
      if l = 0 || l <> Array.length shape || o <> Array.(init (length o) Fun.id)
      then raise Invalid_transpose_order else ()

    let decode o x =
      let inv_order = Array.(make (length o) 0) in
      Array.iteri (fun i x -> inv_order.(x) <- i) o;
      Ndarray.transpose ~axes:inv_order x

    let to_yojson : int array -> Yojson.Safe.t = fun order ->
      let o = `List (List.map (fun x -> `Int x) (Array.to_list order)) in
      `Assoc [("name", `String "transpose"); ("configuration", `Assoc ["order", o])]

    let rec of_yojson :
      int array -> Yojson.Safe.t -> ([`Transpose of int array], string) result
      = fun chunk_shape x ->
      match Yojson.Safe.Util.(member "configuration" x) with
      | `Assoc [("order", `List o)] ->
        let accum a acc = Result.bind acc (add_as_int a) in
        Result.bind (List.fold_right accum o (Ok [])) (to_codec ~chunk_shape)
      | _ -> Error "Invalid transpose configuration."

    and add_as_int v acc = match v with
      | `Int i -> Ok (i :: acc)
      | _ -> Error "transpose order values must be integers."

    and to_codec ~chunk_shape o =
      let order = Array.of_list o in
      match parse ~order chunk_shape with
      | exception Invalid_transpose_order -> Error "Invalid_transpose_order"
      | () -> Ok (`Transpose order)
  end

  let parse : arraytoarray -> int array -> unit = fun t shape -> match t with
    | `Transpose order -> Transpose.parse ~order shape

  let encoded_size : int -> arraytoarray -> int = fun input_size -> function
    | `Transpose _ -> Transpose.encoded_size input_size

  let encoded_repr : int array -> arraytoarray -> int array = fun shape t -> match t with
    | `Transpose order -> Transpose.encoded_repr ~order shape

  let encode : 'a Ndarray.t -> arraytoarray -> 'a Ndarray.t = fun x -> function
    | `Transpose order -> Transpose.encode order x

  let decode : arraytoarray -> 'a Ndarray.t -> 'a Ndarray.t = fun t x -> match t with
    | `Transpose order -> Transpose.decode order x

  let to_yojson : arraytoarray -> Yojson.Safe.t = function
    | `Transpose order -> Transpose.to_yojson order

  let of_yojson cs x = match Util.get_name x with
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

    let to_yojson : compression_level -> Yojson.Safe.t = fun l ->
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
    let parse_clevel l = if l < min_clevel || max_clevel < l then (raise Invalid_zstd_level)

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
          | () -> Ok (`Zstd (l, c))
          | exception Invalid_zstd_level -> Error "Invalid_zstd_level"
        end
      | _ -> Error "Invalid Zstd configuration."
  end

  let encoded_size : int -> fixed_bytestobytes -> int = fun input -> function
    | `Crc32c -> Crc32c.encoded_size input

  let parse : bytestobytes -> unit = function
    | `Zstd (l, _) -> Zstd.parse_clevel l
    | (`Gzip _ | `Crc32c) -> ()

  let encode : string -> bytestobytes -> string = fun x -> function
    | `Gzip l -> Gzip.encode l x
    | `Crc32c -> Crc32c.encode x
    | `Zstd (l, c) -> Zstd.encode l c x

  let decode : bytestobytes -> string -> string = fun t x -> match t with
    | `Gzip _ -> Gzip.decode x
    | `Crc32c -> Crc32c.decode x
    | `Zstd _ -> Zstd.decode x

  let to_yojson : bytestobytes -> Yojson.Safe.t = function
    | `Gzip l -> Gzip.to_yojson l
    | `Crc32c -> Crc32c.to_yojson 
    | `Zstd (l, c) -> Zstd.to_yojson l c

  let of_yojson : Yojson.Safe.t -> (bytestobytes, string) result = fun x -> match Util.get_name x with
    | "gzip" -> Gzip.of_yojson x
    | "crc32c" -> Crc32c.of_yojson x
    | "zstd" -> Zstd.of_yojson x
    | s -> Error (Printf.sprintf "codec %s is not supported." s)
end

module ArrayMap = Util.ArrayMap
module RegularGrid = Extensions.RegularGrid

module rec ArrayToBytes : sig
  module Make (IO : Types.IO) : sig
    type t = internal_shard_config
    type get_partial_values = Types.range list -> string list IO.t
    type set_fn = ?append:bool -> (int * string) list -> unit IO.t
    val partial_encode : t -> get_partial_values -> set_fn -> int -> 'a array_repr -> (int array * 'a) list -> 'a -> unit IO.t
    val partial_decode : t -> get_partial_values -> int -> 'a array_repr -> (int * int array) list -> 'a -> (int * 'a) list IO.t
  end
  val parse : arraytobytes -> int array -> unit
  val encoded_size : int -> fixed_arraytobytes -> int
  val encode : arraytobytes -> 'a Ndarray.t -> string
  val decode : arraytobytes -> 'a array_repr -> string -> 'a Ndarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (arraytobytes, string) result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end = struct

  module Make (IO : Types.IO) = struct
    open IO.Syntax
    open ShardingIndexed

    type t = ShardingIndexed.t
    type get_partial_values = (int * int option) list -> string list IO.t
    type set_fn = ?append:bool -> (int * string) list -> unit IO.t

    let add_binding ~grid acc (c, v) =
      let id, co = RegularGrid.index_coord_pair grid c in
      ArrayMap.add_to_list id (co, v) acc

    (* specialized function for partially writing possibly multiple inner chunks
       to an empty shard of a designated array using the sharding indexed codec.*)
    let partial_encode_empty_shard t (set_partial : set_fn) repr pairs fill_value =
      let update_index ~t ~index ~fill_value ~repr i z (ofs, acc) =
        let arr = Ndarray.create repr.kind t.chunk_shape fill_value in
        List.iter (fun (c, v) -> Ndarray.set arr c v) z;
        let s = encode_chain t.codecs arr in
        let n = String.length s in
        Ndarray.set index (Array.append i [|0|]) (Stdint.Uint64.of_int ofs);
        Ndarray.set index (Array.append i [|1|]) (Stdint.Uint64.of_int n);
        ofs + n, (ofs, s) :: acc
      in
      let cps = Array.map2 (/) repr.shape t.chunk_shape in
      let index = Ndarray.create Uint64 (Array.append cps [|2|]) Stdint.Uint64.max_int in
      let init = match t.index_location with
        | Start -> index_size t.index_codecs cps
        | End -> 0
      in
      (* simulate the inner chunks of a shard as a regular grid of specified shape.*)
      let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
      (* build a finite map with its keys being an inner chunk's index and values
         being a list of (coord-within-inner-chunk, new-value) pairs such that
         new-value is set for the coordinate coord-within-inner-chunk of the inner
         chunk represented by the associated key.*)
      let m = List.fold_left (add_binding ~grid) ArrayMap.empty pairs in
      let f = update_index ~t ~index ~fill_value ~repr in
      let shard_size, ranges = ArrayMap.fold f m (init, []) in
      let indexbytes = encode_index_chain t.index_codecs index in
      (* write all resultant (offset, bytes) pairs into the bytes of the new shard
         taking note to append/prepend the bytes of the shard's index array.*)
      match t.index_location with
      | Start -> set_partial ((0, indexbytes) :: List.rev ranges)
      | End -> set_partial (List.rev @@ (shard_size, indexbytes) :: ranges)

    (* function to partially write new values to one or more inner chunks of
       an existing shard using the sharding indexed codec. *)
    let partial_encode t get_partial (set_partial : set_fn) shardsize repr pairs fv =
      let choose ~idx_arr ((i, _) as bd) =
        let oc = Array.append i [|0|] and nc = Array.append i [|1|] in
        match Ndarray.(get idx_arr oc, get idx_arr nc) with
        | o, n when Stdint.Uint64.(max_int = o && max_int = n) ->
          Either.Left ((-1, None), (oc, nc, -1, 0, bd))
        | o, n ->
          let o', n' = Stdint.Uint64.(to_int o, to_int n) in
          Either.Right ((o', Some n'), (oc, nc, o', n', bd))
      in
      let accumulate_nonempty ~repr' ~idx_arr (acc, l, r) x (oc, nc, ofs, nb, (_, z)) =
        let arr = decode_chain t.codecs repr' x in
        List.iter (fun (c, v) -> Ndarray.set arr c v) z;
        let s = encode_chain t.codecs arr in
        let nb' = String.length s in
        if nb' = nb then acc, (ofs, s) :: l, r else begin
          Ndarray.set idx_arr oc (Stdint.Uint64.of_int acc);
          Ndarray.set idx_arr nc (Stdint.Uint64.of_int nb');
          acc + nb', l, (acc, s) :: r
        end
      in
      let accumulate_empty ~repr' ~idx_arr ~fv (ofs, l) (_, (oc, nc, _, _, (_, z))) =
        let arr = Ndarray.create repr'.kind repr'.shape fv in 
        List.iter (fun (c, v) -> Ndarray.set arr c v) z;
        let s = encode_chain t.codecs arr in
        let n = String.length s in
        Ndarray.set idx_arr oc (Stdint.Uint64.of_int ofs);
        Ndarray.set idx_arr nc (Stdint.Uint64.of_int n);
        ofs + n, (ofs, s) :: l
      in
      (* begin *)
      if shardsize = 0 then partial_encode_empty_shard t set_partial repr pairs fv else
      let cps = Array.map2 (/) repr.shape t.chunk_shape in
      let is = index_size t.index_codecs cps in
      let* l = match t.index_location with
        | Start -> get_partial [0, Some is]
        | End -> get_partial [shardsize - is, None]
      in
      let index_bytes = List.hd l in
      let idx_arr, _ = decode_index t cps index_bytes in
      let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
      let m = List.fold_left (add_binding ~grid) ArrayMap.empty pairs in
      (* split the finite map m into key-value pairs representing empty inner chunks
         and those that don't (using the fact that empty inner chunks have index
         array values equal to 2^64 - 1; then process these seperately.*)
      let empty, nonempty = List.partition_map (choose ~idx_arr) (ArrayMap.bindings m) in
      let ranges, nonempty' = List.split nonempty in
      let* xs = get_partial ranges in
      let repr' = {repr with shape = t.chunk_shape} in
      (* fold over the nonempty index coordinates and finite map to obtain
         (offset, bytes) pairs to write in-place and those to append at the
         end of the shard. bytes to write in-place are determined by comparing
         encoded size vs the corresponding nbytes[i] value already contained in
         the shard's index array.*)
      let bsize, inplace, nonempty_append =
        List.fold_left2 (accumulate_nonempty ~repr' ~idx_arr) (shardsize, [], []) xs nonempty'
      in
      let* () = match inplace with
        | [] -> IO.return_unit
        | rs -> set_partial rs
      in
      let* () = match nonempty_append with
        | [] -> IO.return_unit
        | rs -> set_partial ~append:true (List.rev rs)
      in
      (* new values that need to be written to previously empty inner chunks will
         be appended at the end of the shard and the corresponding index array's
         offset and number-of-bytes values updated accordingly.*)
      let bsize', empty_append =
        List.fold_left (accumulate_empty ~repr' ~idx_arr ~fv) (bsize, []) empty
      in
      let* () = match empty_append with
        | [] -> IO.return_unit
        | rs -> set_partial ~append:true (List.rev rs)
      in
      let ib = encode_index_chain t.index_codecs idx_arr in
      match t.index_location with
      | Start -> set_partial [0, ib]
      | End -> set_partial ~append:true [bsize', ib]
      (* end *)

    type indexed_coord = int * int array 
    (* function to partially read values off of a non-empty shard previously
       encoded using the sharding indexed codec. *) 
    let partial_decode t get_partial shardsize repr (pairs : indexed_coord list) fill_value =
      let add_binding ~grid acc (i, y) =
        let id, c = RegularGrid.index_coord_pair grid y in
        ArrayMap.add_to_list id (i, c) acc
      in
      let choose ~index ((i, _) as bd) =
        match Ndarray.(get index @@ Array.append i [|0|],
                       get index @@ Array.append i [|1|]) with
        | o, n when Stdint.Uint64.(max_int = o && max_int = n) ->
          Either.Left ((-1, None), bd)
        | o, n ->
          let o', n' = Stdint.Uint64.(to_int o, to_int n) in
          Either.Right ((o', Some n'), bd)
      in
      let indexed_chunk_data ~repr' x (_, z) =
        let arr = decode_chain t.codecs repr' x in
        List.map (fun (i, c) -> i, Ndarray.get arr c) (z : indexed_coord list)
      in
      let indexed_empty_chunk (_, (_, z)) =
        List.map (fun (i, _) -> i, fill_value) (z : indexed_coord list)
      in
      let cps = Array.map2 (/) repr.shape t.chunk_shape in
      let is = index_size t.index_codecs cps in
      let* l = match t.index_location with
        | Start -> get_partial [0, Some is]
        | End -> get_partial [shardsize - is, None]
      in
      let index_bytes = List.hd l in
      let index, _ = decode_index t cps index_bytes in
      let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
      let m = List.fold_left (add_binding ~grid) ArrayMap.empty pairs in
      let empty, nonempty = List.partition_map (choose ~index) (ArrayMap.bindings m) in
      let ranges, bindings = List.split nonempty in
      let+ xs = get_partial ranges in
      let repr' = {repr with shape = t.chunk_shape} in
      let res1 = List.concat @@ List.map2 (indexed_chunk_data ~repr') xs bindings in
      let res2 = List.concat_map indexed_empty_chunk empty in  
      res1 @ res2
  end

  let parse : arraytobytes -> int array -> unit = fun t shape -> match t with
    | `Bytes _ -> ()
    | `ShardingIndexed c -> ShardingIndexed.parse c shape

  let encoded_size : int -> fixed_arraytobytes -> int = fun input_size -> function
    | `Bytes _ -> Bytes'.encoded_size input_size

  let encode : arraytobytes -> 'a Ndarray.t -> string = fun t x -> match t with
    | `Bytes endian -> Bytes'.encode x endian
    | `ShardingIndexed c -> ShardingIndexed.encode c x

  let decode : arraytobytes -> 'a array_repr -> string -> 'a Ndarray.t = fun t repr b -> match t with
    | `Bytes endian -> Bytes'.decode b repr endian
    | `ShardingIndexed c -> ShardingIndexed.decode c repr b

  let to_yojson : arraytobytes -> Yojson.Safe.t = function
    | `Bytes endian -> Bytes'.to_yojson endian
    | `ShardingIndexed c -> ShardingIndexed.to_yojson c

  let of_yojson : int array -> Yojson.Safe.t -> (arraytobytes, string) result = fun shp x ->
    match Util.get_name x with
    | "bytes" -> Result.map (fun e -> `Bytes e) (Bytes'.of_yojson x)
    | "sharding_indexed" -> Result.map (fun c -> `ShardingIndexed c) (ShardingIndexed.of_yojson shp x)
    | _ -> Error ("array->bytes codec not supported: ")
end

and Bytes' : sig
  val encoded_size : int -> int
  val encode : 'a Ndarray.t -> endianness -> string
  val decode : string -> 'a array_repr -> endianness -> 'a Ndarray.t
  val of_yojson : Yojson.Safe.t -> (endianness, string) result
  val to_yojson : endianness -> Yojson.Safe.t
end = struct
  let encoded_size : int -> int = Fun.id 

  let endian_module = function
    | LE -> (module Ebuffer.Little : Ebuffer.S)
    | BE -> (module Ebuffer.Big : Ebuffer.S)

  let encode (type a) (x : a Ndarray.t) (e : endianness) : string =
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

  let decode (type a) (str : string) (decoded : a array_repr) (e : endianness) : a Ndarray.t =
    let open (val endian_module e) in
    let k, shape = decoded.kind, decoded.shape in
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

  let to_yojson : endianness -> Yojson.Safe.t = fun e ->
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
  val parse : t -> int array -> unit
  val encode : t -> 'a Ndarray.t -> string
  val decode : t -> 'a array_repr -> string -> 'a Ndarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
  val encode_chain : (arraytobytes, bytestobytes) chain -> 'a Ndarray.t -> string
  val decode_chain : (arraytobytes, bytestobytes) chain -> 'a array_repr -> string -> 'a Ndarray.t
  val decode_index : t -> int array -> string -> Stdint.uint64 Ndarray.t * string
  val index_size : (fixed_arraytobytes, fixed_bytestobytes) chain -> int array -> int
  val encode_index_chain : (fixed_arraytobytes, fixed_bytestobytes) chain -> Stdint.uint64 Ndarray.t -> string
end = struct
  module Indexing = Ndarray.Indexing
  type t = internal_shard_config  

  let parse_chain : int array -> (arraytobytes, bytestobytes) chain -> unit = fun shape chain ->
    let shape' = match chain.a2a with
      | [] -> shape
      | x :: _ as xs ->
        ArrayToArray.parse x shape;
        List.fold_left ArrayToArray.encoded_repr shape xs
    in
    ArrayToBytes.parse chain.a2b shape'

  let parse t shape =
    if Array.(length shape <> length t.chunk_shape)
    || not @@ Array.for_all2 (fun x y -> (x mod y) = 0) shape t.chunk_shape
    then raise Invalid_sharding_chunk_shape else
    parse_chain shape t.codecs;
    let index_shape = Array.append shape [|2|] in
    parse_chain index_shape (t.index_codecs :> (arraytobytes, bytestobytes) chain)

  let encoded_size init chain =
    let a2a_size = List.fold_left ArrayToArray.encoded_size init chain.a2a in
    let a2b_size = ArrayToBytes.encoded_size a2a_size chain.a2b in
    List.fold_left BytesToBytes.encoded_size a2b_size chain.b2b
  
  let encode_chain chain x =
    let a = List.fold_left ArrayToArray.encode x chain.a2a in
    let b = ArrayToBytes.encode chain.a2b a in
    List.fold_left BytesToBytes.encode b chain.b2b

  let encode_index_chain : (fixed_arraytobytes, fixed_bytestobytes) chain -> Stdint.uint64 Ndarray.t -> string = fun t x ->
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
      ArrayMap.add_to_list k (c, Ndarray.get arr coord) acc
    in
    let update_inner_chunk ~t ~shard_idx ~kind idx pairs (ofs, xs) =
      let v = Array.of_list (List.map snd pairs) in
      let x' = Ndarray.of_array kind t.chunk_shape v in
      let b = encode_chain t.codecs x' in
      let nb = Stdint.Uint64.of_int @@ String.length b in
      Ndarray.set shard_idx (Array.append idx [|0|]) ofs;
      Ndarray.set shard_idx (Array.append idx [|1|]) nb;
      Stdint.Uint64.(ofs + nb), b :: xs
    in
    let shard_shape = Ndarray.shape x in
    let cps = Array.map2 (/) shard_shape t.chunk_shape in
    let idx_shp = Array.append cps [|2|] in
    let shard_idx = Ndarray.create Uint64 idx_shp Stdint.Uint64.max_int in
    let grid = RegularGrid.create ~array_shape:shard_shape t.chunk_shape in
    let kind = Ndarray.data_type x in
    let coords = Indexing.coords_of_slice [||] shard_shape in
    let m = Array.fold_right (add_coord ~grid ~arr:x) coords ArrayMap.empty in
    let _, xs = ArrayMap.fold (update_inner_chunk ~t ~shard_idx ~kind) m (Stdint.Uint64.zero, []) in
    let idx_bytes = encode_index_chain t.index_codecs shard_idx in
    match t.index_location with
    | Start -> String.concat String.empty (idx_bytes :: List.rev xs)
    | End -> String.concat String.empty (List.rev (idx_bytes :: xs))

  let decode_chain t repr x =
    let shape = List.fold_left ArrayToArray.encoded_repr repr.shape t.a2a in
    let b2b = List.fold_right BytesToBytes.decode t.b2b x in
    let a2b = ArrayToBytes.decode t.a2b {repr with shape} b2b in
    List.fold_right ArrayToArray.decode t.a2a a2b

  let decode_index_chain : (fixed_arraytobytes, fixed_bytestobytes) chain -> int array -> string -> Stdint.uint64 Ndarray.t = fun t shape x ->
    let shape' = List.fold_left ArrayToArray.encoded_repr shape t.a2a in
    let y = List.fold_right BytesToBytes.decode (t.b2b :> bytestobytes list) x in
    let arr = match t.a2b with
      | `Bytes e -> Bytes'.decode y {shape=shape'; kind=Uint64} e in
    match t.a2a with
    | [] -> arr
    | `Transpose o :: _ ->
      let inv_order = Array.(make (length o) 0) in
      Array.iteri (fun i x -> inv_order.(x) <- i) o;
      Ndarray.transpose ~axes:inv_order arr

  let index_size index_chain cps = encoded_size (16 * Util.prod cps) index_chain

  let decode_index t cps b =
    let l = index_size t.index_codecs cps in
    let o = String.length b - l in
    let ib, rest = match t.index_location with
      | End -> String.sub b o l, String.sub b 0 o
      | Start -> String.sub b 0 l, String.sub b l o
    in
    let idx_shape = Array.append cps [|2|] in
    decode_index_chain t.index_codecs idx_shape ib, rest

  let decode (type a) (t : t) (repr : a array_repr) (b : string) =
    let add_indexed_coord ~grid acc (i, coord) =
      let k, c = RegularGrid.index_coord_pair grid coord in
      ArrayMap.add_to_list k (i, c) acc
    in
    let read_inner_chunk ~t ~idx_arr ~inner_repr ~chunk_bytes (idx, pairs) =
      let oc = Array.append idx [|0|] in
      let nc = Array.append idx [|1|] in
      let ofs = Stdint.Uint64.to_int (Ndarray.get idx_arr oc) in
      let nb = Stdint.Uint64.to_int (Ndarray.get idx_arr nc) in
      let arr = decode_chain t.codecs inner_repr (String.sub chunk_bytes ofs nb) in
      List.map (fun (i, c) -> i, Ndarray.get arr c) pairs
    in
    let cps = Array.map2 (/) repr.shape t.chunk_shape in
    let idx_arr, chunk_bytes = decode_index t cps b in
    let grid = RegularGrid.create ~array_shape:repr.shape t.chunk_shape in
    let coords = Indexing.coords_of_slice [||] repr.shape in
    let indexed_coords = Array.mapi (fun i v -> i, v) coords in
    let m = Array.fold_left (add_indexed_coord ~grid) ArrayMap.empty indexed_coords in
    let inner_repr = {repr with shape = t.chunk_shape} in
    let bd = ArrayMap.bindings m in
    let pairs = List.concat_map (read_inner_chunk ~t ~idx_arr ~inner_repr ~chunk_bytes) bd in
    let sorted_pairs = List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) pairs in
    let vs = List.map snd sorted_pairs in
    Ndarray.of_array inner_repr.kind repr.shape (Array.of_list vs)

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
    let chunk_shape = `List (List.map (fun x -> `Int x) @@ Array.to_list t.chunk_shape) in
    `Assoc
    [("name", `String "sharding_indexed");
     ("configuration", `Assoc
      [("chunk_shape", chunk_shape);
       ("index_location", index_location);
       ("index_codecs", index_codecs);
       ("codecs", chain_to_yojson t.codecs)])]

  let chain_of_yojson chunk_shape codecs =
    let open Util.Result_syntax in
    let split ~f codec (l, r) = 
      Result.fold ~ok:(fun v -> v :: l, r) ~error:(fun _ -> l, codec :: r) (f codec)
    in
    let partition f encoded = List.fold_right (split ~f) encoded ([], []) in
    let* codecs = match codecs with
      | [] -> Error "No codec chain specified for sharding_indexed."
      | y -> Ok y
    in
    let* a2b, rest = match partition (ArrayToBytes.of_yojson chunk_shape) codecs with
      | [x], rest -> Ok (x, rest)
      | _ -> Error "Must be exactly one array->bytes codec."
    in
    let a2a, rest = partition (ArrayToArray.of_yojson chunk_shape) rest in
    let b2b, rest = partition BytesToBytes.of_yojson rest in
    match rest with
    | [] -> Ok {a2a; a2b; b2b}
    | x :: _ ->
      let codec = Util.get_name x in
      Error (Printf.sprintf "%s codec is unsupported or has invalid configuration." codec)

  let of_yojson shard_shape x =
    let open Util.Result_syntax in
    let extract ~assoc name =
      Yojson.Safe.Util.filter_map (fun (n, v) -> if n = name then Some v else None) assoc
    in
    let add_as_int a acc =
      let* k = acc in
      match a with
      | `Int i when i > 0 -> Ok (i :: k)
      | _ -> Error "chunk_shape must only contain positive integers."
    in
    let add_fixed_size_codec ~error_msg c acc =
      let* l = acc in
      match c with
      | `Crc32c -> Ok (`Crc32c :: l)
      | `Gzip _ | `Zstd _ -> Error error_msg
    in
    let assoc = Yojson.Safe.Util.(member "configuration" x |> to_assoc) in
    let* l' = match extract ~assoc "chunk_shape" with
      | [] -> Error "sharding_indexed must contain a chunk_shape field"
      | x :: _ -> List.fold_right add_as_int (Yojson.Safe.Util.to_list x) (Ok [])
    in
    let chunk_shape = Array.of_list l' in
    let* index_location = match extract ~assoc "index_location" with
      | [] -> Error "sharding_indexed must have a index_location field"
      | x :: _ ->
        match x with
        | `String "end" -> Ok End
        | `String "start" -> Ok Start
        | _ -> Error "index_location must only be 'end' or 'start'"
    in
    let* codecs = match extract ~assoc "codecs" with
      | [] -> Error "sharding_indexed must have a codecs field"
      | x :: _ -> chain_of_yojson chunk_shape (Yojson.Safe.Util.to_list x)
    in
    let* ic = match extract ~assoc "index_codecs" with
      | [] -> Error "sharding_indexed must have a index_codecs field"
      | x :: _ ->
        let cps = Array.map2 (/) shard_shape chunk_shape in
        let idx_shape = Array.append cps [|2|] in
        chain_of_yojson idx_shape (Yojson.Safe.Util.to_list x)
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
  {chunk_shape : int array
  ;codecs : codec list
  ;index_codecs : index_codec list
  ;index_location : loc}

module Chain = struct
  type t = (arraytobytes, bytestobytes) chain

  let rec create shape chain =
    let a2a, rest = extract_arraytoarray [] chain in
    let a2b, rest = extract_arraytobytes shape rest in
    let b2b, other = extract_bytestobytes [] rest in
    if List.compare_length_with other 0 <> 0 then raise Invalid_codec_ordering else
    (* At this point the codec ordering in the chain is correct, so parse its contents.*)
    let a2a_encoded_shape = match a2a with
      | [] -> shape
      | x :: _ as xs -> 
        ArrayToArray.parse x shape;
        List.fold_left ArrayToArray.encoded_repr shape xs
    in
    ArrayToBytes.parse a2b a2a_encoded_shape;
    List.iter BytesToBytes.parse b2b;
    {a2a; a2b; b2b}

  and extract_arraytoarray l r = match r with
    | (#arraytoarray as x) :: xs -> extract_arraytoarray (l @ [x]) xs
    | xs -> (l, xs)

  and extract_bytestobytes l r = match r with
    | (#bytestobytes as x) :: xs -> extract_bytestobytes (l @ [x]) xs
    | xs -> (l, xs)

  and extract_arraytobytes shape = function
    | (#fixed_arraytobytes as x) :: xs -> (x, xs)
    | (#variable_array_tobytes as x) :: xs ->
      begin match x with
      | `ShardingIndexed cfg ->
        let codecs = create shape cfg.codecs in
        let index_codecs = create (Array.append shape [|2|]) (cfg.index_codecs :> codec list) in
        (* coerse to a fixed codec chain list type *)
        let pred = function #fixed_bytestobytes as c -> Some c | _ -> None in
        let b2b = List.filter_map pred index_codecs.b2b in
        let a2b = match index_codecs.a2b with
          | #fixed_arraytobytes as c -> c
          | _ -> raise Array_to_bytes_invariant 
        in
        let cfg' : internal_shard_config = {
          index_codecs = {index_codecs with a2b; b2b};
          index_location = cfg.index_location;
          chunk_shape = cfg.chunk_shape;
          codecs;
        }
        in (`ShardingIndexed cfg', xs)
      end
    | _ -> raise Array_to_bytes_invariant
 
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

  let of_yojson chunk_shape x =
    let open Util.Result_syntax in
    let split ~f codec (l, r) = 
      Result.fold ~ok:(fun v -> v :: l, r) ~error:(fun _ -> l, codec :: r) (f codec)
    in
    let partition f encoded = List.fold_right (split ~f) encoded ([], []) in
    let* codecs = match Yojson.Safe.Util.to_list x with
      | [] -> Error "No codec specified."
      | y -> Ok y
    in
    let* a2b, rest = match partition (ArrayToBytes.of_yojson chunk_shape) codecs with
      | [x], rest -> Ok (x, rest)
      | _ -> Error "Must be exactly one array->bytes codec."
    in
    let a2a, rest = partition (ArrayToArray.of_yojson chunk_shape) rest in
    let b2b, rest = partition BytesToBytes.of_yojson rest in
    match rest with
    | [] -> Ok {a2a; a2b; b2b}
    | x :: _ ->
      let codec = Util.get_name x in
      Error (Printf.sprintf "%s codec is unsupported or has invalid configuration." codec)
end

module Make (IO : Types.IO) = struct
  module M = ArrayToBytes.Make(IO)

  let is_just_sharding : Chain.t -> bool = function
    | {a2a = []; a2b = `ShardingIndexed _; b2b = []} -> true
    | _ -> false

  let partial_encode t f g bsize repr pairs fv = match t.a2b with
    | `ShardingIndexed c -> M.partial_encode c f g bsize repr pairs fv
    | `Bytes _ -> failwith "bytes codec does not support partial encoding." 

  let partial_decode t f s repr pairs fv = match t.a2b with
    | `ShardingIndexed c -> M.partial_decode c f s repr pairs fv
    | `Bytes _ -> failwith "bytes codec does not support partial decoding."
end
