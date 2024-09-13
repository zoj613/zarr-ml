open Extensions

exception Parse_error of string

module FillValue = struct
  type t =
    | Char of char
    | Bool of bool
    | Int of Stdint.uint64
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
  : type a. a Ndarray.dtype -> a -> t
  = fun kind a ->
    match kind with
    | Ndarray.Char -> Char a
    | Ndarray.Int8 -> Int (Stdint.Uint64.of_int a)
    | Ndarray.Uint8 -> Int (Stdint.Uint64.of_int a)
    | Ndarray.Int16 -> Int (Stdint.Uint64.of_int a)
    | Ndarray.Uint16 -> Int (Stdint.Uint64.of_int a)
    | Ndarray.Int32 -> Int (Stdint.Uint64.of_int32 a)
    | Ndarray.Int64 -> Int (Stdint.Uint64.of_int64 a)
    | Ndarray.Uint64 -> Int a
    | Ndarray.Float32 -> Float a
    | Ndarray.Float64 -> Float a
    | Ndarray.Complex32 -> FloatComplex a
    | Ndarray.Complex64 -> FloatComplex a
    | Ndarray.Int -> Int (Stdint.Uint64.of_int a)
    | Ndarray.Nativeint -> Int (Stdint.Uint64.of_nativeint a)

  let rec of_yojson x =
    match x with
    | `Bool b -> Ok (Bool b)
    | `Int i -> Result.ok @@ Int (Stdint.Uint64.of_int i)
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
        Result.bind (of_yojson a) @@ fun x ->
        Result.bind (of_yojson b) @@ fun y ->
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
    | Int i -> `Int (Stdint.Uint64.to_int i)
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
    ;storage_transformers : Yojson.Safe.t list}

  let create
    ?(sep=`Slash)
    ?(dimension_names=[])
    ?(attributes=`Null)
    ~codecs
    ~shape
    kind
    fv
    chunks
    =
    {shape
    ;codecs
    ;attributes
    ;dimension_names
    ;zarr_format = 3
    ;node_type = "array"
    ;storage_transformers = []
    ;fill_value = FillValue.of_kind kind fv
    ;data_type = Datatype.of_kind kind
    ;chunk_key_encoding = ChunkKeyEncoding.create sep
    ;chunk_grid = RegularGrid.create ~array_shape:shape chunks}

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
    match t.dimension_names with
    | [] -> `Assoc l
    | xs  ->
      let xs' = List.map (Option.fold ~none:`Null ~some:(fun s -> `String s)) xs in
      `Assoc (l @ [("dimension_names", `List xs')])

  let of_yojson x =
    let open Yojson.Safe.Util in
    let open Util.Result_syntax in
    let* zarr_format = match member "zarr_format" x with
      | `Int (3 as i) -> Ok i
      | `Null -> Error "array metadata must contain a zarr_format field."
      | _ -> Error "zarr_format field must be the integer 3."
    in
    let* node_type = match member "node_type" x with
      | `String ("array" as a) -> Ok a
      | `Null -> Error "array metadata must contain a node_type field."
      | _ -> Error "node_type field must be 'array'."
    in
    let* shape = match member "shape" x with
      | `List xs ->
        let+ l = List.fold_right
          (fun a acc ->
            let* k = acc in
            match a with
            | `Int i when i > 0 -> Ok (i :: k)
            | _ ->
              Result.error "shape field list must only contain positive integers.")
          xs (Ok []) in Array.of_list l
       | `Null -> Error "array metadata must contain a shape field."
       | _ -> Error "shape field must be a list of integers."
    in
    let* data_type = match member "data_type" x with
      | `String _ as c -> Datatype.of_yojson c
      | `Null -> Error "array metadata must contain a data_type field."
      | _ -> Error "data_type field must be a string."
    in
    let* chunk_shape, chunk_grid = match member "chunk_grid" x with
      | `Null -> Error "array metadata must contain a chunk_grid field." 
      | xs ->
        match Util.get_name xs, member "configuration" xs with
        | "regular", `Assoc [("chunk_shape", `List l)] ->
          let* v = List.fold_right
            (fun a acc ->
              let* k = acc in
              match a with 
              | `Int i when i > 0 -> Ok (i :: k)
              | _ ->
                Error "chunk_shape must only contain positive ints.") l (Ok []) in
          let cs = Array.of_list v in
          let+ r = match RegularGrid.create ~array_shape:shape cs with
            | exception RegularGrid.Grid_shape_mismatch -> Error "grid shape mismatch."
            | g -> Ok g
          in cs, r
        | _ -> Error "Invalid Chunk grid name or configuration."
    in
    let* codecs = match member "codecs" x with
      | `List _ as c -> Codecs.Chain.of_yojson chunk_shape c
      | `Null -> Error "array metadata must contain a codecs field."
      | _ -> Error "codecs field must be a list of objects."
    in
    let* fill_value = match member "fill_value" x with
      | `Null -> Error "array metadata must contain a fill_value field."
      | xs -> FillValue.of_yojson xs
    in
    let* chunk_key_encoding = match member "chunk_key_encoding" x with 
      | `Null ->
        Error "array metadata must contain a chunk_key_encoding field." 
      | xs -> ChunkKeyEncoding.of_yojson xs
    in
    (* Optional fields *)
    let attributes = member "attributes" x in
    let* dimension_names = match member "dimension_names" x with
      | `Null -> Ok []
      | `List xs ->
        if List.length xs <> Array.length shape then
          Error ("dimension_names length and array dimensionality must be equal.")
        else
          List.fold_right
            (fun a acc ->
              let* k = acc in
              match a with
              | `String s -> Ok (Some s :: k)
              | `Null -> Ok (None :: k)
              | _ -> Error "dimension_names must contain strings or null values.") xs (Ok [])
      | _ -> Error "dimension_names field must be a list."
    in
    let+ storage_transformers = match member "storage_transformers" x with
      | `Null -> Ok []
      | _ -> Error "storage_transformers field is not yet supported."
    in
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

  let dimension_names t = t.dimension_names

  let attributes t = t.attributes

  let chunk_shape t =
    RegularGrid.chunk_shape t.chunk_grid

  let index_coord_pair t coord =
    RegularGrid.index_coord_pair t.chunk_grid coord

  let chunk_key t index =
    ChunkKeyEncoding.encode t.chunk_key_encoding index

  let chunk_indices t shape =
    RegularGrid.indices t.chunk_grid shape

  let encode t =
    Yojson.Safe.to_string @@ to_yojson t

  let decode s = 
    match of_yojson @@ Yojson.Safe.from_string s with
    | Ok m -> m
    | Error e -> raise @@ Parse_error e

  let update_attributes t attrs =
    {t with attributes = attrs}

  let update_shape t shape = {t with shape}

  let is_valid_kind
    : type a. t -> a Ndarray.dtype -> bool
    = fun t kind ->
    match kind, t.data_type with
    | Ndarray.Char, Datatype.Char
    | Ndarray.Int8, Datatype.Int8
    | Ndarray.Uint8, Datatype.Uint8
    | Ndarray.Int16, Datatype.Int16
    | Ndarray.Uint16, Datatype.Uint16
    | Ndarray.Int32, Datatype.Int32
    | Ndarray.Int64, Datatype.Int64
    | Ndarray.Uint64, Datatype.Uint64
    | Ndarray.Float32, Datatype.Float32
    | Ndarray.Float64, Datatype.Float64
    | Ndarray.Complex32, Datatype.Complex32
    | Ndarray.Complex64, Datatype.Complex64
    | Ndarray.Int, Datatype.Int
    | Ndarray.Nativeint, Datatype.Nativeint -> true
    | _ -> false

  let fillvalue_of_kind
    : type a. t -> a Ndarray.dtype -> a
    = fun t kind ->
    match kind, t.fill_value with
    | Ndarray.Char, FillValue.Char c -> c
    | Ndarray.Int8, FillValue.Int i -> Stdint.Uint64.to_int i
    | Ndarray.Uint8, FillValue.Int i -> Stdint.Uint64.to_int i
    | Ndarray.Int16, FillValue.Int i -> Stdint.Uint64.to_int i
    | Ndarray.Uint16, FillValue.Int i -> Stdint.Uint64.to_int i
    | Ndarray.Int32, FillValue.Int i -> Stdint.Uint64.to_int32 i
    | Ndarray.Int64, FillValue.Int i -> Stdint.Uint64.to_int64 i
    | Ndarray.Uint64, FillValue.Int i -> i 
    | Ndarray.Int, FillValue.Int i -> Stdint.Uint64.to_int i
    | Ndarray.Nativeint, FillValue.Int i -> Stdint.Uint64.to_nativeint i
    | Ndarray.Float32, FillValue.Float f -> f 
    | Ndarray.Float32, FillValue.FloatBits f -> f 
    | Ndarray.Float64, FillValue.Float f -> f 
    | Ndarray.Float64, FillValue.FloatBits f -> f 
    | Ndarray.Complex32, FillValue.IntComplex c -> c
    | Ndarray.Complex32, FillValue.FloatComplex c -> c
    | Ndarray.Complex32, FillValue.FFComplex c -> c
    | Ndarray.Complex32, FillValue.FBComplex c -> c
    | Ndarray.Complex32, FillValue.BFComplex c -> c
    | Ndarray.Complex32, FillValue.BBComplex c -> c
    | Ndarray.Complex64, FillValue.IntComplex c -> c
    | Ndarray.Complex64, FillValue.FloatComplex c -> c
    | Ndarray.Complex64, FillValue.FFComplex c -> c
    | Ndarray.Complex64, FillValue.FBComplex c -> c
    | Ndarray.Complex64, FillValue.BFComplex c -> c
    | Ndarray.Complex64, FillValue.BBComplex c -> c
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
      ;("node_type", `String t.node_type)] in
    match t.attributes with
    | `Null -> `Assoc l
    | x -> `Assoc (l @ [("attributes", x)])

  let of_yojson x =
    let open Yojson.Safe.Util in
    let open Util.Result_syntax in
    let* zarr_format = match member "zarr_format" x with
      | `Int (3 as i) -> Ok i
      | `Null -> Error "group metadata must contain a zarr_format field."
      | _ -> Error "zarr_format field must be the integer 3."
    in
    let+ node_type = match member "node_type" x with
      | `String ("group" as g) -> Ok g
      | `Null -> Error "group metadata must contain a node_type field."
      | _ -> Error "node_type field must be 'group."
    in
    let attributes = match member "attributes" x with
      | `Null -> `Null 
      | xs -> xs
    in {zarr_format; node_type; attributes}

  let decode s = 
    match of_yojson @@ Yojson.Safe.from_string s with
    | Ok m -> m
    | Error e -> raise @@ Parse_error e

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
