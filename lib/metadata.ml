type error =
  [ `Json_decode_error of string ]

module FillValue = struct
  type t =
    | Char of char
    | Bool of bool
    | Int of int64
    | Float of float
    | FloatBits of float
    | IntComplex of Complex.t
    | FloatComplex of Complex.t
    | FFComplex of Complex.t
    | FBComplex of Complex.t
    | BFComplex of Complex.t
    | BBComplex of Complex.t

  let equal x y = x = y

  let of_kind
  : type a b. (a, b) Bigarray.kind -> a -> t
  = fun kind a ->
    match kind with
    | Bigarray.Char -> Char a
    | Bigarray.Int8_signed -> Int (Int64.of_int a)
    | Bigarray.Int8_unsigned -> Int (Int64.of_int a)
    | Bigarray.Int16_signed -> Int (Int64.of_int a)
    | Bigarray.Int16_unsigned -> Int (Int64.of_int a)
    | Bigarray.Int32 -> Int (Int64.of_int32 a)
    | Bigarray.Int64 -> Int a
    | Bigarray.Float32 -> Float a
    | Bigarray.Float64 -> Float a
    | Bigarray.Complex32 -> FloatComplex a
    | Bigarray.Complex64 -> FloatComplex a
    | Bigarray.Int -> Int (Int64.of_int a)
    | Bigarray.Nativeint -> Int (Int64.of_nativeint a)

  let rec of_yojson x =
    let open Util.Result_syntax in
    match x with
    | `Bool b -> Ok (Bool b)
    | `Int i -> Result.ok @@ Int (Int64.of_int i)
    | `Float f -> Ok (Float f)
    | `String "Infinity" ->
      Ok (Float Float.infinity)
    | `String "-Infinity" ->
      Ok (Float Float.neg_infinity)
    | `String "NaN" ->
      Ok (Float Float.nan)
    | `String s when String.length s = 1 ->
      Ok (Char (String.get s 0))
    | `String s when String.starts_with ~prefix:"0x" s ->
      let b = Int64.of_string s in
      Ok (FloatBits (Int64.float_of_bits b))
    | `List [`Int x; `Int y] ->
      let re = Float.of_int x
      and im = Float.of_int y in
      Ok (IntComplex Complex.{re; im})
    | `List [`Float re; `Float im] ->
      Ok (FloatComplex Complex.{re; im})
    | `List [`String _ as a; `String _ as b] ->
        of_yojson a >>= fun x ->
        of_yojson b >>= fun y ->
        (match x, y with
        | Float re, Float im ->
          Ok (FFComplex Complex.{re; im})
        | Float re, FloatBits im ->
          Ok (FBComplex Complex.{re; im})
        | FloatBits re, Float im ->
          Ok (BFComplex Complex.{re; im})
        | FloatBits re, FloatBits im ->
          Ok (BBComplex Complex.{re; im})
        | _ -> Error "Unsupported fill value")
    | _ -> Error "Unsupported fill value."

  let rec to_yojson = function
    | Bool b -> `Bool b
    | Int i -> `Int (Int64.to_int i)
    | Char c ->
      `String (String.of_seq @@ List.to_seq [c])
    | Float f when f = Float.infinity ->
      `String "Infinity"
    | Float f when f = Float.neg_infinity ->
      `String "-Infinity"
    | Float f when f = Float.nan ->
      `String "NaN"
    | Float f -> `Float f
    | FloatBits f ->
      `String (Stdint.Int64.to_string_hex @@ Int64.bits_of_float f)
    | IntComplex Complex.{re; im} ->
      `List [`Int (Float.to_int re); `Int (Float.to_int im)]
    | FloatComplex Complex.{re; im} ->
      `List [`Float re; `Float im]
    | FFComplex Complex.{re; im} ->
      `List [to_yojson (Float re); to_yojson (Float im)]
    | FBComplex Complex.{re; im} ->
      `List [to_yojson (Float re); to_yojson (FloatBits im)]
    | BFComplex Complex.{re; im} ->
      `List [to_yojson (FloatBits re); to_yojson (Float im)]
    | BBComplex Complex.{re; im} ->
      `List [to_yojson (FloatBits re); to_yojson (FloatBits im)]
end

module ArrayMetadata = struct 
  type t =
    {zarr_format : int
    ;shape : int array
    ;node_type : string
    ;data_type : Extensions.Datatype.t
    ;codecs : Codecs.Chain.t
    ;fill_value : FillValue.t
    ;chunk_grid : Extensions.RegularGrid.t
    ;chunk_key_encoding : Extensions.ChunkKeyEncoding.t
    ;attributes : Yojson.Safe.t [@default `Null]
    ;dimension_names : string option list [@default []]
    ;storage_transformers : Yojson.Safe.t Util.ExtPoint.t list [@default []]}
  [@@deriving yojson, eq]

  let create
    ?(sep=Extensions.Slash)
    ?(codecs=Codecs.Chain.default)
    ?(dimension_names=[])
    ?(attributes=`Null)
    ~shape
    kind
    fv
    chunks
    =
    {shape
    ;codecs
    ;fill_value = FillValue.of_kind kind fv
    ;data_type = Extensions.Datatype.of_kind kind
    ;chunk_grid = Extensions.RegularGrid.create chunks
    ;chunk_key_encoding = Extensions.ChunkKeyEncoding.create sep
    ;zarr_format = 3
    ;node_type = "array"
    ;attributes
    ;dimension_names
    ;storage_transformers = []}

  let shape t = t.shape

  let codecs t = t.codecs

  let data_type t =
    Yojson.Safe.to_string @@
    Extensions.Datatype.to_yojson t.data_type

  let ndim t = Array.length @@ shape t

  let dimension_names t = t.dimension_names

  let attributes t = t.attributes

  let chunk_shape t =
    Extensions.RegularGrid.chunk_shape t.chunk_grid

  let grid_shape t shape =
    Extensions.RegularGrid.grid_shape t.chunk_grid shape

  let index_coord_pair t coord =
    Extensions.RegularGrid.index_coord_pair t.chunk_grid coord

  let chunk_key t index =
    Extensions.ChunkKeyEncoding.encode t.chunk_key_encoding index

  let chunk_indices t shape =
    Extensions.RegularGrid.indices t.chunk_grid shape

  let encode t =
    Yojson.Safe.to_string @@ to_yojson t

  let decode b = 
    let open Util.Result_syntax in
    of_yojson @@ Yojson.Safe.from_string b >>? fun s ->
    `Json_decode_error s

  let update_attributes t attrs =
    {t with attributes = attrs}

  let update_shape t shape = {t with shape}

  let is_valid_kind
    : type a b. t -> (a, b) Bigarray.kind -> bool
    = fun t kind ->
    match kind, t.data_type with
    | Bigarray.Char, Extensions.Datatype.Char
    | Bigarray.Int8_signed, Extensions.Datatype.Int8
    | Bigarray.Int8_unsigned, Extensions.Datatype.Uint8
    | Bigarray.Int16_signed, Extensions.Datatype.Int16
    | Bigarray.Int16_unsigned, Extensions.Datatype.Uint16
    | Bigarray.Int32, Extensions.Datatype.Int32
    | Bigarray.Int64, Extensions.Datatype.Int64
    | Bigarray.Float32, Extensions.Datatype.Float32
    | Bigarray.Float64, Extensions.Datatype.Float64
    | Bigarray.Complex32, Extensions.Datatype.Complex32
    | Bigarray.Complex64, Extensions.Datatype.Complex64 -> true
    | _ -> false

  let fillvalue_of_kind
    : type a b. t -> (a, b) Bigarray.kind -> a
    = fun t kind ->
    match kind, t.fill_value with
    | Bigarray.Char, FillValue.Char c -> c
    | Bigarray.Int8_signed, FillValue.Int i -> Int64.to_int i
    | Bigarray.Int8_unsigned, FillValue.Int i -> Int64.to_int i
    | Bigarray.Int16_signed, FillValue.Int i -> Int64.to_int i
    | Bigarray.Int16_unsigned, FillValue.Int i -> Int64.to_int i
    | Bigarray.Int32, FillValue.Int i -> Int64.to_int32 i
    | Bigarray.Int64, FillValue.Int i -> i 
    | Bigarray.Float32, FillValue.Float f -> f 
    | Bigarray.Float32, FillValue.FloatBits f -> f 
    | Bigarray.Float64, FillValue.Float f -> f 
    | Bigarray.Float64, FillValue.FloatBits f -> f 
    | Bigarray.Complex32, FillValue.IntComplex c -> c
    | Bigarray.Complex32, FillValue.FloatComplex c -> c
    | Bigarray.Complex32, FillValue.FFComplex c -> c
    | Bigarray.Complex32, FillValue.FBComplex c -> c
    | Bigarray.Complex32, FillValue.BFComplex c -> c
    | Bigarray.Complex32, FillValue.BBComplex c -> c
    | Bigarray.Complex64, FillValue.IntComplex c -> c
    | Bigarray.Complex64, FillValue.FloatComplex c -> c
    | Bigarray.Complex64, FillValue.FFComplex c -> c
    | Bigarray.Complex64, FillValue.FBComplex c -> c
    | Bigarray.Complex64, FillValue.BFComplex c -> c
    | Bigarray.Complex64, FillValue.BBComplex c -> c
    | _ -> failwith "kind is not compatible with node's fill value."
end

module GroupMetadata = struct
  type t =
    {zarr_format : int
    ;node_type : string
    ;attributes : Yojson.Safe.t [@default `Null]}
  [@@deriving yojson, show]

  let default =
    {zarr_format = 3; node_type = "group"; attributes = `Null}

  let decode s = 
    let open Util.Result_syntax in
    of_yojson @@ Yojson.Safe.from_string s >>? fun s ->
    `Json_decode_error s

  let encode t =
    Yojson.Safe.to_string @@ to_yojson t

  let update_attributes t attrs =
    {t with attributes = attrs}

  let attributes t = t.attributes
end
