open Extensions
open Util.Result_syntax

type error =
  [ Extensions.error
  | `Json_decode of string ]

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

  let ( = ) x y = x = y

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
    match x with
    | `Bool b -> Ok (Bool b)
    | `Int i -> Result.ok @@ Int (Int64.of_int i)
    | `String "Infinity" -> Ok (Float Float.infinity)
    | `String "-Infinity" -> Ok (Float Float.neg_infinity)
    | `String "NaN" -> Ok (Float Float.nan)
    | `Float f -> Ok (Float f)
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
        | _ -> Error "Unsupported fill value.")
    | _ -> Error "Unsupported fill value."

  let rec to_yojson = function
    | Bool b -> `Bool b
    | Int i -> `Int (Int64.to_int i)
    | Char c ->
      `String (String.of_seq @@ List.to_seq [c])
    | Float f when Float.is_nan f ->
      `String "NaN"
    | Float f when f = Float.infinity ->
      `String "Infinity"
    | Float f when f = Float.neg_infinity ->
      `String "-Infinity"
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
    ;data_type : Datatype.t
    ;codecs : Codecs.Chain.t
    ;fill_value : FillValue.t
    ;chunk_grid : RegularGrid.t
    ;chunk_key_encoding : ChunkKeyEncoding.t
    ;attributes : Yojson.Safe.t
    ;dimension_names : string option list
    ;storage_transformers : Yojson.Safe.t Util.ExtPoint.t list}

  let create
    ?(sep=`Slash)
    ?(codecs=Codecs.Chain.default)
    ?(dimension_names=[])
    ?(attributes=`Null)
    ~shape
    kind
    fv
    chunks
    =
    RegularGrid.create ~array_shape:shape chunks
    >>| fun chunk_grid ->
    {shape
    ;codecs
    ;chunk_grid
    ;attributes
    ;dimension_names
    ;zarr_format = 3
    ;node_type = "array"
    ;storage_transformers = []
    ;fill_value = FillValue.of_kind kind fv
    ;data_type = Datatype.of_kind kind
    ;chunk_key_encoding = ChunkKeyEncoding.create sep}

  let to_yojson t =
    let shape =
      Array.map (fun x -> `Int x) t.shape |> Array.to_list in
    let l =
      [("zarr_format", `Int t.zarr_format)
      ;("shape", `List shape)
      ;("node_type", `String t.node_type)
      ;("data_type", Datatype.to_yojson t.data_type)
      ;("codecs", Codecs.Chain.to_yojson t.codecs)
      ;("fill_value", FillValue.to_yojson t.fill_value)
      ;("chunk_grid", RegularGrid.to_yojson t.chunk_grid)
      ;("chunk_key_encoding",
        ChunkKeyEncoding.to_yojson t.chunk_key_encoding)]
    in
    let l =
      match t.attributes with
      | `Null -> l
      | x -> l @ [("attributes", x)]
    in
    let l =
      match t.dimension_names with
      | [] -> l
      | xs  ->
        let xs' =
          List.map (function
            | Some s -> `String s
            | None -> `Null) xs
        in
        l @ [("dimension_names", `List xs')]
    in `Assoc l

  let of_yojson x =
    let open Yojson.Safe.Util in
    (match member "zarr_format" x with
    | `Int (3 as i) -> Ok i
    | `Null -> Error "array metadata must contain a zarr_format field."
    | _ -> Error "zarr_format field must be the integer 3.")
    >>= fun zarr_format ->

    (match member "node_type" x with
    | `String ("array" as a) -> Ok a
    | `Null -> Error "array metadata must contain a node_type field."
    | _ -> Error "node_type field must be 'array'.")
    >>= fun node_type ->

    (* An array can have zero or more dimensions. *)
    (match member "shape" x with
    | `List xs ->
      List.fold_right
        (fun a acc ->
            acc >>= fun k ->
            match a with
            | `Int i when i > 0 -> Ok (i :: k)
            | _ ->
              Result.error @@
              "shape field list must only contain positive integers.")
        xs (Ok [])
        >>| Array.of_list
     | `Null -> Error "array metadata must contain a shape field."
     | _ -> Error "shape field must be a list of integers.")
    >>= fun shape ->

    (match member "data_type" x with
    | `String _ as c -> Datatype.of_yojson c
    | `Null -> Error "array metadata must contain a data_type field."
    | _ -> Error "data_type field must be a string.")
    >>= fun data_type ->

    (match member "codecs" x with
    | `List _ as c -> Codecs.Chain.of_yojson c
    | `Null -> Error "array metadata must contain a codecs field."
    | _ -> Error "codecs field must be a list of objects.")
    >>= fun codecs ->

    (match member "fill_value" x with
    | `Null -> Error "array metadata must contain a fill_value field."
    | xs -> FillValue.of_yojson xs)
    >>= fun fill_value ->

    (match member "chunk_grid" x with
    | `Null -> Error "array metadata must contain a chunk_grid field." 
    | xs ->
      RegularGrid.of_yojson xs >>= fun grid -> 
      RegularGrid.(create ~array_shape:shape @@ chunk_shape grid)
      >>? fun (`Grid {msg; _}) -> msg)
    >>= fun chunk_grid ->

    (match member "chunk_key_encoding" x with 
    | `Null ->
      Error "array metadata must contain a chunk_key_encoding field." 
    | xs -> ChunkKeyEncoding.of_yojson xs)
    >>= fun chunk_key_encoding ->

    (* Optional fields *)
    let attributes = member "attributes" x in

    (match member "dimension_names" x with
    | `Null -> Ok []
    | `List xs ->
      if List.length xs <> Array.length shape then
        Result.error
        "dimension_names length and array dimensionality must be equal."
      else
        List.fold_right
          (fun a acc ->
              acc >>= fun k ->
              match a with
              | `String s -> Ok (Some s :: k)
              | `Null -> Ok (None :: k)
              | _ ->
                let msg =
                  "dimension_names must contain strings or null values."
                in Error msg) xs (Ok [])
    | _ -> Error "dimension_names field must be a list.")
    >>= fun dimension_names ->

    (match member "storage_transformers" x with
    | `Null -> Ok []
    | _ -> Error "storage_transformers field is not yet supported.")
    >>| fun storage_transformers ->

    {zarr_format; shape; node_type; data_type; codecs; fill_value; chunk_grid
    ;chunk_key_encoding; attributes; dimension_names; storage_transformers}

  let ( = ) x y =
    x.zarr_format = y.zarr_format
    && x.shape = y.shape
    && x.node_type = y.node_type
    && Datatype.(x.data_type = y.data_type)
    && Codecs.Chain.(x.codecs = y.codecs)
    && FillValue.(x.fill_value = y.fill_value)
    && RegularGrid.(x.chunk_grid = y.chunk_grid)
    && ChunkKeyEncoding.(x.chunk_key_encoding = y.chunk_key_encoding)
    && x.attributes = y.attributes
    && x.dimension_names = y.dimension_names
    && x.storage_transformers = y.storage_transformers

  let shape t = t.shape

  let codecs t = t.codecs

  let ndim t = Array.length @@ shape t

  let dimension_names t = t.dimension_names

  let attributes t = t.attributes

  let chunk_shape t =
    RegularGrid.chunk_shape t.chunk_grid

  let grid_shape t shape =
    RegularGrid.grid_shape t.chunk_grid shape

  let index_coord_pair t coord =
    RegularGrid.index_coord_pair t.chunk_grid coord

  let chunk_key t index =
    ChunkKeyEncoding.encode t.chunk_key_encoding index

  let chunk_indices t shape =
    RegularGrid.indices t.chunk_grid shape

  let encode t =
    Yojson.Safe.to_string @@ to_yojson t

  let decode b = 
    of_yojson @@ Yojson.Safe.from_string b >>? fun s ->
    `Json_decode s

  let update_attributes t attrs =
    {t with attributes = attrs}

  let update_shape t shape = {t with shape}

  let is_valid_kind
    : type a b. t -> (a, b) Bigarray.kind -> bool
    = fun t kind ->
    match kind, t.data_type with
    | Bigarray.Char, Datatype.Char
    | Bigarray.Int8_signed, Datatype.Int8
    | Bigarray.Int8_unsigned, Datatype.Uint8
    | Bigarray.Int16_signed, Datatype.Int16
    | Bigarray.Int16_unsigned, Datatype.Uint16
    | Bigarray.Int32, Datatype.Int32
    | Bigarray.Int64, Datatype.Int64
    | Bigarray.Float32, Datatype.Float32
    | Bigarray.Float64, Datatype.Float64
    | Bigarray.Complex32, Datatype.Complex32
    | Bigarray.Complex64, Datatype.Complex64
    | Bigarray.Int, Datatype.Int
    | Bigarray.Nativeint, Datatype.Nativeint -> true
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
    | Bigarray.Int, FillValue.Int i -> Int64.to_int i
    | Bigarray.Nativeint, FillValue.Int i -> Int64.to_nativeint i
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
    ;attributes : Yojson.Safe.t}

  let default =
    {zarr_format = 3; node_type = "group"; attributes = `Null}

  let to_yojson t =
    let l =
      [("zarr_format", `Int t.zarr_format)
      ;("node_type", `String t.node_type)]
    in
    let l =
      match t.attributes with
      | `Null -> l
      | x -> l @ [("attributes", x)]
    in `Assoc l

  let of_yojson x =
    let open Yojson.Safe.Util in
    (match member "zarr_format" x with
    | `Int (3 as i) -> Ok i
    | `Null -> Error "group metadata must contain a zarr_format field."
    | _ -> Error "zarr_format field must be the integer 3.")
    >>= fun zarr_format ->
    (match member "node_type" x with
    | `String ("group" as g) -> Ok g
    | `Null -> Error "group metadata must contain a node_type field."
    | _ -> Error "node_type field must be 'group.")
    >>= fun node_type ->
    let attributes =
      match member "attributes" x with
      | `Null -> `Null 
      | xs -> xs
    in
    Ok {zarr_format; node_type; attributes}

  let decode s = 
    of_yojson @@ Yojson.Safe.from_string s >>? fun b ->
    `Json_decode b

  let encode t =
    Yojson.Safe.to_string @@ to_yojson t

  let update_attributes t attrs =
    {t with attributes = attrs}

  let attributes t = t.attributes

  let show t =
    Format.sprintf
      {|"{zarr_format=%d; node_type=%s; attributes=%s}"|}
      t.zarr_format t.node_type @@ Yojson.Safe.show t.attributes
end
