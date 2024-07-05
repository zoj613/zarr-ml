open Array_to_array
open Bytes_to_bytes
open Util.Result_syntax

module Ndarray = Owl.Dense.Ndarray.Generic

type endianness =
  | Little
  | Big
  [@@deriving show]

type loc =
  | Start
  | End
  [@@deriving show]

type array_to_bytes =
  | Bytes of endianness
  | ShardingIndexed of shard_config
  [@@deriving show]

and shard_config =
  {chunk_shape : int array
  ;codecs : chain
  ;index_codecs : chain
  ;index_location : loc}

and chain =
  {a2a: array_to_array list
  ;a2b: array_to_bytes
  ;b2b: bytes_to_bytes list}
  [@@deriving show]

type error =
  [ `Bytes_encode_error of string
  | `Bytes_decode_error of string
  | `Sharding_shape_mismatch of int array * int array * string
  | Array_to_array.error
  | Bytes_to_bytes.error ]

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/bytes/v1.0.html *)
module BytesCodec = struct
  let compute_encoded_size (input_size : int) = input_size

  let endian_module = function
    | Little -> (module Ebuffer.Little : Ebuffer.S)
    | Big -> (module Ebuffer.Big : Ebuffer.S)

  let encode
    : type a b. (a, b) Ndarray.t -> endianness -> (string, [> error]) result
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

  let decode 
    : type a b.
      string ->
      (a, b) Util.array_repr ->
      endianness ->
      ((a, b) Ndarray.t, [> error]) result
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
      | Little -> "little"
      | Big -> "big"
    in
    `Assoc
    [("name", `String "bytes")
    ;("configuration", `Assoc ["endian", `String endian])]

  let of_yojson x =
    match Yojson.Safe.Util.(member "configuration" x |> to_assoc) with
    | [("endian", `String e)] ->
      (match e with
      | "little" -> Ok Little
      | "big" -> Ok Big
      | s ->
        Result.error @@ "Unsupported bytes endianness: " ^ s)
    | _ -> Error "Invalid bytes codec configuration."
end

module rec ArrayToBytes : sig
  val parse
    : ('a, 'b) Util.array_repr ->
      array_to_bytes ->
      (unit, [> error]) result
  val compute_encoded_size : int -> array_to_bytes -> int
  val default : array_to_bytes
  val encode
    : ('a, 'b) Ndarray.t ->
      array_to_bytes ->
      (string, [> error]) result
  val decode
    : string ->
      ('a, 'b) Util.array_repr ->
      array_to_bytes ->
      (('a, 'b) Ndarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (array_to_bytes, string) result
  val to_yojson : array_to_bytes -> Yojson.Safe.t
end = struct

  let default = Bytes Little

  let parse decoded_repr = function
    | Bytes _ -> Ok ()
    | ShardingIndexed c ->
      ShardingIndexedCodec.parse decoded_repr c

  let compute_encoded_size input_size = function
    | Bytes _ ->
      BytesCodec.compute_encoded_size input_size
    | ShardingIndexed s ->
      ShardingIndexedCodec.compute_encoded_size input_size s

  let encode
    : type a b.
      (a, b) Ndarray.t ->
      array_to_bytes ->
      (string, [> error]) result
    = fun x -> function
    | Bytes endian -> BytesCodec.encode x endian
    | ShardingIndexed c -> ShardingIndexedCodec.encode x c

  let decode
    : type a b. 
      string ->
      (a, b) Util.array_repr ->
      array_to_bytes ->
      ((a, b) Ndarray.t, [> error]) result
    = fun b repr -> function
    | Bytes endian -> BytesCodec.decode b repr endian
    | ShardingIndexed c -> ShardingIndexedCodec.decode b repr c

  let to_yojson = function
    | Bytes endian -> BytesCodec.to_yojson endian
    | ShardingIndexed c -> ShardingIndexedCodec.to_yojson c

  let of_yojson x =
    match Util.get_name x with
    | "bytes" ->
      BytesCodec.of_yojson x >>| fun e -> Bytes e
    | "sharding_indexed" ->
      ShardingIndexedCodec.of_yojson x >>| fun c -> ShardingIndexed c
    | _ -> Error ("array->bytes codec not supported: ")
end

and ShardingIndexedCodec : sig
  type t = shard_config
  val parse
    : ('a, 'b) Util.array_repr ->
      t ->
      (unit, [> error]) result
  val compute_encoded_size : int -> t -> int
  val encode
    : ('a, 'b) Ndarray.t ->
      t ->
      (string, [> error]) result
  val decode
    : string ->
      ('a, 'b) Util.array_repr ->
      t ->
      (('a, 'b) Ndarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
end = struct

  type t = shard_config  

  let parse 
    : type a b.
      (a, b) Util.array_repr ->
      shard_config ->
      (unit, [> error]) result
    = fun repr t ->
    (match Array.(length repr.shape = length t.chunk_shape) with
    | true -> Ok ()
    | false ->
      let msg =
        "sharding chunk_shape length must equal the dimensionality of
        the decoded representaton of a shard." in
      Result.error @@
      `Sharding_shape_mismatch (t.chunk_shape, repr.shape, msg))
    >>= fun () ->
    match
      Array.for_all2 (fun x y -> (x mod y) = 0) repr.shape t.chunk_shape
    with
    | true -> Ok ()
    | false ->
      let msg =
        "sharding chunk_shape must evenly divide the size of the shard shape."
      in
      Result.error @@
      `Sharding_shape_mismatch (t.chunk_shape, repr.shape, msg)

  let compute_encoded_size input_size t =
    List.fold_left BytesToBytes.compute_encoded_size
      (ArrayToBytes.compute_encoded_size
         (List.fold_left
            ArrayToArray.compute_encoded_size
            input_size t.index_codecs.a2a)
         t.index_codecs.a2b)
      t.index_codecs.b2b
  
  let rec encode_chain
    : type a b.
      chain ->
      (a, b) Ndarray.t ->
      (string, [> error]) result
    = fun t x ->
    List.fold_left
      (fun acc c -> acc >>= ArrayToArray.encode c) (Ok x) t.a2a
    >>= fun y ->
    List.fold_left
      (fun acc c -> acc >>= BytesToBytes.encode c)
      (ArrayToBytes.encode y t.a2b) t.b2b

  and encode
    : type a b.
      (a, b) Ndarray.t ->
      shard_config ->
      (string, [> error]) result
    = fun x t ->
    let open Util in
    let open Util.Result_syntax in
    let shard_shape = Ndarray.shape x in
    let cps = Array.map2 (/) shard_shape t.chunk_shape in
    let idx_shp = Array.append cps [|2|] in
    let shard_idx = 
      Ndarray.create Bigarray.Int64 idx_shp Int64.max_int in
    let sg =
      Extensions.RegularGrid.create t.chunk_shape in
    let slice =
      Array.make
        (Ndarray.num_dims x) (Owl_types.R []) in
    let coords = Indexing.coords_of_slice slice shard_shape in
    let tbl = Arraytbl.create @@ Array.length coords in
    Ndarray.iteri (fun i y ->
      let k, c = Extensions.RegularGrid.index_coord_pair sg coords.(i) in
      Arraytbl.add tbl k (c, y)) x;
    let fill_value = 
      Arraytbl.to_seq_values tbl
      |> Seq.uncons
      |> Option.get
      |> fst
      |> snd
    in
    let repr =
      {kind = Ndarray.kind x
      ;fill_value
      ;shape = t.chunk_shape} in
    let cindices = ArraySet.of_seq @@ Arraytbl.to_seq_keys tbl in
    let buf = Buffer.create @@ Ndarray.size_in_bytes x in
    let offset = ref 0L in
    let coord = idx_shp in
    ArraySet.fold (fun idx acc ->
      acc >>= fun () ->
      (* find_all returns bindings in reverse order. To restore the
       * C-ordering of elements we must call List.rev. *)
      let vals =
        Arraytbl.find_all tbl idx
        |> List.rev 
        |> List.split
        |> snd
        |> Array.of_list
      in
      let x' = Ndarray.of_array repr.kind vals t.chunk_shape in
      encode_chain t.codecs x' >>| fun b ->
      Buffer.add_string buf b;
      let len = Array.length idx in
      Array.blit idx 0 coord 0 len;
      coord.(len) <- 0;
      Ndarray.set shard_idx coord !offset;
      coord.(len) <- 1;
      let nbytes = Int64.of_int @@ String.length b in
      Ndarray.set shard_idx coord nbytes;
      offset := Int64.add !offset nbytes) cindices (Ok ())
    >>= fun () ->
    encode_chain t.index_codecs shard_idx >>| fun b' ->
    match t.index_location with
    | Start ->
      let buf' = Buffer.create @@ String.length b' in
      Buffer.add_string buf' b';
      Buffer.add_buffer buf' buf;
      Buffer.contents buf'
    | End ->
      Buffer.add_string buf b';
      Buffer.contents buf

  let rec decode_chain
    : type a b. 
      chain ->
      string ->
      (a, b) Util.array_repr ->
      ((a, b) Ndarray.t, [> error]) result
    = fun t x repr ->
    List.fold_right
      (fun c acc -> acc >>= BytesToBytes.decode c) t.b2b (Ok x)
    >>= fun y ->
    List.fold_right
      (fun c acc -> acc >>= ArrayToArray.decode c)
      t.a2a (ArrayToBytes.decode y repr t.a2b)

  and decode_index
    : string ->
      int array ->
      shard_config ->
      ((int64, Bigarray.int64_elt) Ndarray.t * string, [> error]) result
    = fun b shard_shape t ->
    let open Util in
    let cps = Array.map2 (/) shard_shape t.chunk_shape in
    let l = index_size t cps in
    let o = String.length b - l in
    let b', rest =
      match t.index_location with
      | End -> String.sub b o l, String.sub b 0 o
      | Start -> String.sub b 0 l, String.sub b l o
    in
    let repr =
      {fill_value = Int64.max_int
      ;kind = Bigarray.Int64
      ;shape = Array.append cps [|2|]}
    in
    decode_chain t.index_codecs b' repr >>| fun decoded ->
    (decoded, rest)

  and index_size t cps =
    compute_encoded_size (16 * Util.prod cps) t

  and decode
    : type a b. 
      string ->
      (a, b) Util.array_repr ->
      t ->
      ((a, b) Ndarray.t, [> error]) result
    = fun b repr t ->
    let open Util in
    let open Extensions in
    let open Util.Result_syntax in
    decode_index b repr.shape t >>= fun (shard_idx, b') ->
    if Ndarray.for_all (Int64.equal Int64.max_int) shard_idx then
      Ok (Ndarray.create repr.kind repr.shape repr.fill_value)
    else
      let sg = RegularGrid.create t.chunk_shape in
      let slice =
        Array.make
          (Array.length repr.shape)
          (Owl_types.R []) in
      (* pair (i, c) is a pair of shard chunk index (i) and shard coordinate c *)
      let pair =
        Array.map
        (RegularGrid.index_coord_pair sg)
        (Indexing.coords_of_slice slice repr.shape) in
      let tbl = Arraytbl.create @@ Array.length pair in
      let inner =
        {kind = repr.kind
        ;shape = t.chunk_shape
        ;fill_value = repr.fill_value}
    in
    Array.fold_right (fun (idx, coord) acc ->
      acc >>= fun l ->
      match Arraytbl.find_opt tbl idx with
      | Some arr ->
        Ok (Ndarray.get arr coord :: l)
      | None ->
        match Ndarray.(slice_left shard_idx idx) with
        | pair when Ndarray.for_all (Int64.equal Int64.max_int) pair ->
          let x = Ndarray.create inner.kind inner.shape inner.fill_value in
          Arraytbl.add tbl idx x;
          Ok (Ndarray.get x coord :: l)
        | pair ->
          let p = Bigarray.array1_of_genarray pair in
          let c = String.sub b' (Int64.to_int p.{0}) (Int64.to_int p.{1}) in
          decode_chain t.codecs c inner >>= fun x ->
          Arraytbl.add tbl idx x;
          Ok (Ndarray.get x coord :: l)) pair (Ok [])
    >>| fun res -> 
    Ndarray.of_array
      inner.kind (Array.of_list res) repr.shape

  let rec chain_to_yojson chain =
    [%to_yojson: Yojson.Safe.t list] @@
    List.map ArrayToArray.to_yojson chain.a2a @
    (ArrayToBytes.to_yojson chain.a2b) ::
    List.map BytesToBytes.to_yojson chain.b2b

  and to_yojson t =
    let codecs = chain_to_yojson t.codecs
    in
    let index_codecs = chain_to_yojson t.index_codecs
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

  let rec chain_of_yojson
  : Yojson.Safe.t list -> (chain, string) result
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
        | `Int i -> Ok (i :: k)
        | _ -> Error "chunk_shape must only contain integers.")
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
    >>| fun index_codecs ->
    {index_codecs; index_location; codecs; chunk_shape}
end
