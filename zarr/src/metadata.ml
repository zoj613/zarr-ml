open Extensions

exception Parse_error of string

module FillValue = struct
  type t =
    | Char of char
    | Bool of bool
    | Int of int
    | Intlit of string * Stdint.uint64 (* for ints that cannot fit in a 63bit integer type *)
    | Float of float
    | IntFloat of int * float
    | IntlitFloat of string * float
    | StringFloat of string * float  (* float represented using hex string in the metadata json. *)
    | IntComplex of (int * int) * Complex.t  (* complex number represented using ints in the metadata json. *)
    | IntlitComplex of (string * string) * Complex.t  (* complex number represented using ints in the metadata json. *)
    | FloatComplex of Complex.t  (* complex number represented using floats in the metadata json. *)
    | StringComplex of (string * string) * Complex.t

  let rec create : type a. a Ndarray.dtype -> a -> t = fun kind x -> match kind with
    | Ndarray.Char -> Char x
    | Ndarray.Bool -> Bool x
    | Ndarray.Int8 -> Int x
    | Ndarray.Uint8 -> Int x
    | Ndarray.Int16 -> Int x
    | Ndarray.Uint16 -> Int x
    | Ndarray.Int32 -> Int (Int32.to_int x)
    | Ndarray.Int -> Int x
    | Ndarray.Int64 when x >= -4611686018427387904L && x <= 4611686018427387903L -> Int (Int64.to_int x)
    | Ndarray.Int64 -> Intlit (Int64.to_string x, Stdint.Uint64.of_int64 x)
    | Ndarray.Uint64 when Stdint.Uint64.(compare x (of_int Int.max_int)) < 0 -> Int (Stdint.Uint64.to_int x)
    | Ndarray.Uint64 -> Intlit (Stdint.Uint64.to_string x, x)
    | Ndarray.Float32 -> Float x
    | Ndarray.Float64 -> Float x
    | Ndarray.Complex32 -> FloatComplex x
    | Ndarray.Complex64 -> FloatComplex x
    | Ndarray.Nativeint -> create Ndarray.Int64 (Int64.of_nativeint x)

  let equal x y = match x, y with
    | Char a, Char b when Char.equal a b -> true
    | Bool false, Bool false -> true
    | Bool true, Bool true -> true
    | Int a, Int b when Int.equal a b -> true
    | Intlit (a, _), Intlit (b, _) when String.equal a b -> true
    | Float a, Float b when Float.equal a b -> true
    | IntFloat (a, _), IntFloat (b, _) when Int.equal a b -> true
    | IntlitFloat (a, _), IntlitFloat (b, _) when String.equal a b -> true
    | StringFloat ("Infinity", _), StringFloat ("Infinity", _) -> true
    | StringFloat ("-Infinity", _), StringFloat ("-Infinity", _) -> true
    | StringFloat ("NaN", _), StringFloat ("NaN", _) -> true
    | StringFloat (a, _), StringFloat (b, _) when String.equal a b -> true
    | IntComplex ((a1, b1), _), IntComplex ((a2, b2), _) when Int.(equal a1 a2 && equal b1 b2) -> true
    | IntlitComplex ((a1, b1), _), IntlitComplex ((a2, b2), _) when String.(equal a1 a2 && equal b1 b2) -> true
    | FloatComplex Complex.{re=r1;im=i1}, FloatComplex Complex.{re=r2;im=i2} when Float.(equal r1 r2 && equal i1 i2) -> true
    | StringComplex ((a1, b1), _), StringComplex ((a2, b2), _) when String.(equal a1 a2 && equal b1 b2) -> true
    | _ -> false

  (* This makes sure the way the fill-value is encoded in the metadata is
     preserved when converting a parsed FillValue.t back to it's JSON value. *)
  let rec of_yojson (d : Datatype.t) (x : Yojson.Safe.t) = match d, x with
    | Datatype.Char, `String s when String.length s = 1 -> Ok (Char (String.get s 0))
    | Datatype.Bool, `Bool b -> Ok (Bool b)
    | Datatype.Int8, `Int a when a >= -128 && a <= 127 -> Ok (Int a)
    | Datatype.Uint8, `Int a when a >= 0 && a <= 255 -> Ok (Int a)
    | Datatype.Int16, `Int a when a >= -32768 && a <= 32767 -> Ok (Int a)
    | Datatype.Uint16, `Int a when a >= 0 && a <= 65535 -> Ok (Int a)
    | Datatype.Int32, `Int a when a >= -2147483648 && a <= 2147483647 -> Ok (Int a)
    | Datatype.Int, `Int a -> Ok (Int a)
    | Datatype.Int64, `Int a -> Ok (Int a)
    | Datatype.Int64, `Intlit a -> begin match Int64.of_string_opt a with
      | None -> Error "Unsupported fill value."
      | Some b -> Ok (Intlit (a, Stdint.Uint64.of_int64 b)) end
    | Datatype.Nativeint, a -> of_yojson Datatype.Int64 a
    | Datatype.Uint64, `Int a when a >= 0 -> Ok (Int a)
    | Datatype.Uint64, `Intlit a when not (String.starts_with ~prefix:"-" a) ->
      begin match Stdint.Uint64.of_string a with
      | exception Invalid_argument _ -> Error "Unsupported fill value."
      | b -> Ok (Intlit (a, b)) end
    | Datatype.Float32, `Float a -> Ok (Float a)
    | Datatype.Float32, `Int a -> Ok (IntFloat (a, Float.of_int a))
    | Datatype.Float32, `Intlit a -> Ok (IntlitFloat (a, Float.of_string a))
    | Datatype.Float32, `String ("Infinity" as s) -> Ok (StringFloat (s, Float.infinity))
    | Datatype.Float32, `String ("-Infinity" as s) -> Ok (StringFloat (s, Float.neg_infinity))
    | Datatype.Float32, `String ("NaN" as s) -> Ok (StringFloat (s, Float.nan))
    | Datatype.Float32, `String s when String.starts_with ~prefix:"0x" s ->
      begin match Stdint.Uint64.of_string s with
      | exception Invalid_argument _ -> Error "Unsupported fill value."
      | a -> Ok (StringFloat (s, Stdint.Uint64.to_float a)) end
    | Datatype.Float64, `Float a -> Ok (Float a)
    | Datatype.Float64, `Int a -> Ok (IntFloat (a, Float.of_int a))
    | Datatype.Float64, `Intlit a -> Ok (IntlitFloat (a, Float.of_string a))
    | Datatype.Float64, `String ("Infinity" as s) -> Ok (StringFloat (s, Float.infinity))
    | Datatype.Float64, `String ("-Infinity" as s) -> Ok (StringFloat (s, Float.neg_infinity))
    | Datatype.Float64, `String ("NaN" as s) -> Ok (StringFloat (s, Float.nan))
    | Datatype.Float64, `String s when String.starts_with ~prefix:"0x" s ->
      begin match Stdint.Uint64.of_string s with
      | exception Invalid_argument _ -> Error "Unsupported fill value."
      | a -> Ok (StringFloat (s, Stdint.Uint64.to_float a)) end
    | Datatype.Complex32, `List [`Int a; `Int b] -> Ok (IntComplex ((a, b), Complex.{re=Float.of_int a; im=Float.of_int b}))
    | Datatype.Complex32, `List [`Intlit a; `Intlit b] -> Ok (IntlitComplex ((a, b), Complex.{re=Float.of_string a; im=Float.of_string b}))
    | Datatype.Complex32, `List [`Float re; `Float im] -> Ok (FloatComplex Complex.{re; im})
    | Datatype.Complex32, `List [`String a; `String b] -> Ok (StringComplex ((a, b), Complex.{re=Float.of_string a; im=Float.of_string b}))
    | Datatype.Complex64, `List [`Int a; `Int b] -> Ok (IntComplex ((a, b), Complex.{re=Float.of_int a; im=Float.of_int b}))
    | Datatype.Complex64, `List [`Intlit a; `Intlit b] -> Ok (IntlitComplex ((a, b), Complex.{re=Float.of_string a; im=Float.of_string b}))
    | Datatype.Complex64, `List [`Float re; `Float im] -> Ok (FloatComplex Complex.{re; im})
    | Datatype.Complex64, `List [`String a; `String b] -> Ok (StringComplex ((a, b), Complex.{re=Float.of_string a; im=Float.of_string b}))
    | _, `Null -> Error "array metadata must contain a fill_value field."
    | _ -> Error "Unsupported fill value."

  let to_yojson : t -> Yojson.Safe.t = function
    | Char c -> `String (Printf.sprintf "%c" c)
    | Bool b -> `Bool b
    | Int i -> `Int i
    | Intlit (s, _) -> `Intlit s
    | Float f -> `Float f
    | IntFloat (i, _) -> `Int i
    | IntlitFloat (s, _) -> `Intlit s
    | StringFloat (s, _) -> `String s
    | IntComplex ((a, b), _) -> `List [`Int a; `Int b]
    | IntlitComplex ((a, b), _) -> `List [`Intlit a; `Intlit b]
    | FloatComplex Complex.{re; im} -> `List [`Float re; `Float im]
    | StringComplex ((a, b), _) -> `List [`String a; `String b]
end

module NodeType = struct
  type t = Array | Group
  let rec to_yojson x : Yojson.Safe.t = `String (show x)
  and show = function
    | Array -> "array"
    | Group -> "group"

  module Array = struct
    let of_yojson : Yojson.Safe.t -> (t, string) result = function
      | `String "array" -> Ok Array
      | `Null -> Error "metadata must contain a node_type field."
      | _ -> Error "node_type field must be 'array'."
  end

  module Group = struct
    let of_yojson : Yojson.Safe.t -> (t, string) result = function
      | `String "group" -> Ok Group
      | `Null -> Error "group metadata must contain a node_type field."
      | _ -> Error "node_type field must be 'group'."
  end
end

(* The shape of a Zarr array is the list of dimension lengths. It can be the
   empty list in the case of a zero-dimension array (scalar). *)
module Shape = struct
  type t = Empty | Dims of int list

  let create = function
    | [] -> Empty
    | xs -> Dims xs

  let ( = ) x y = match x, y with
    | Empty, Empty -> true
    | Dims a, Dims b when List.equal Int.equal a b -> true
    | _ -> false

  let add (x : Yojson.Safe.t) acc = match x with
    | `Int i when i > 0 -> Result.map (List.cons i) acc
    | _ -> Error "shape field list must only contain positive integers."

  let of_yojson : Yojson.Safe.t -> (t, string) result = function
    | `List [] -> Ok Empty
    | `List xs -> Result.map (fun x -> Dims x) (List.fold_right add xs (Ok []))
    | `Null -> Error "array metadata must contain a shape field."
    | _ -> Error "shape field must be a list of integers."

  let to_yojson : t -> Yojson.Safe.t = function
    | Empty -> `List []
    | Dims xs -> `List (List.map (fun x -> `Int x) xs)

  let to_list = function
    | Empty -> []
    | Dims xs -> xs

  let ndim = function
    | Empty -> 0
    | Dims xs -> List.length xs
end

module ZarrFormat = struct
  type t = int
  let to_yojson x : Yojson.Safe.t = `Int x
  let of_yojson = function
    | `Int (3 as i) -> Ok i
    | `Null -> Error "metadata must contain a zarr_format field."
    | _ -> Error "zarr_format field must be the integer 3."
end

module DimensionNames = struct
  type t = string option list 

  let to_yojson (xs : t) : Yojson.Safe.t =
    `List (List.map (Option.fold ~none:`Null ~some:(fun s -> `String s)) xs)

  let add (x : Yojson.Safe.t) acc = match x with
    | `String s -> Result.map (List.cons (Some s)) acc
    | `Null -> Result.map (List.cons None) acc
    | _ -> Error "dimension_names must contain strings or null values."

  let of_yojson ndim x = match x with
    | `Null -> Ok []
    | `List xs ->
      if List.length xs = ndim then List.fold_right add xs (Ok [])
      else Error "dimension_names length and array dimensionality must be equal."
    | _ -> Error "dimension_names field must be a list."
end

module Array = struct 
  type t =
    {zarr_format : ZarrFormat.t
    ;shape : Shape.t
    ;node_type : NodeType.t
    ;data_type : Datatype.t
    ;codecs : Codecs.Chain.t
    ;fill_value : FillValue.t
    ;chunk_grid : RegularGrid.t
    ;chunk_key_encoding : ChunkKeyEncoding.t
    ;attributes : Yojson.Safe.t
    ;dimension_names : DimensionNames.t
    ;storage_transformers : Yojson.Safe.t list}

  let create ?(sep=`Slash) ?(dimension_names=[]) ?(attributes=`Null) ~codecs ~shape kind fv chunks =
    {codecs
    ;attributes
    ;dimension_names
    ;zarr_format = 3
    ;shape = Shape.create shape
    ;node_type = NodeType.Array
    ;storage_transformers = []
    ;fill_value = FillValue.create kind fv
    ;data_type = Datatype.of_kind kind
    ;chunk_key_encoding = ChunkKeyEncoding.create sep
    ;chunk_grid = RegularGrid.create ~array_shape:shape chunks}

  let to_yojson : t -> Yojson.Safe.t = fun t ->
    let l =
      [("zarr_format", ZarrFormat.to_yojson t.zarr_format)
      ;("shape", Shape.to_yojson t.shape)
      ;("node_type", NodeType.to_yojson t.node_type)
      ;("data_type", Datatype.to_yojson t.data_type)
      ;("codecs", Codecs.Chain.to_yojson t.codecs)
      ;("fill_value", FillValue.to_yojson t.fill_value)
      ;("chunk_grid", RegularGrid.to_yojson t.chunk_grid)
      ;("chunk_key_encoding", ChunkKeyEncoding.to_yojson t.chunk_key_encoding)]
    in
    (* optional fields.*)
    match t.attributes, t.dimension_names with
    | `Null, [] -> `Assoc l
    | `Null, xs -> `Assoc (l @ ["dimension_names", DimensionNames.to_yojson xs])
    | x, [] -> `Assoc (l @ ["attributes", x])
    | x, xs -> `Assoc (l @ [("attributes", x); ("dimension_names", DimensionNames.to_yojson xs)])

  let of_yojson x =
    let open Util.Result_syntax in
    let member = Yojson.Safe.Util.member in
    let* zarr_format = ZarrFormat.of_yojson (member "zarr_format" x) in
    let* shape = Shape.of_yojson (member "shape" x) in
    let* data_type = Datatype.of_yojson (member "data_type" x) in
    let* fill_value = FillValue.of_yojson data_type (member "fill_value" x) in
    let* chunk_key_encoding = ChunkKeyEncoding.of_yojson (member "chunk_key_encoding" x) in
    let* chunk_grid = RegularGrid.of_yojson (Shape.to_list shape) (member "chunk_grid" x) in
    let* codecs = Codecs.Chain.of_yojson (RegularGrid.chunk_shape chunk_grid) (member "codecs" x) in
    let* node_type = NodeType.Array.of_yojson (member "node_type" x) in
    (* Optional fields *)
    let* dimension_names = DimensionNames.of_yojson (Shape.ndim shape) (member "dimension_names" x) in
    let+ storage_transformers = match member "storage_transformers" x with
      | `Null -> Ok []
      | _ -> Error "storage_transformers field is not yet supported."
    in
    let attributes = member "attributes" x in
    {zarr_format; shape; node_type; data_type; codecs; fill_value; chunk_grid
    ;chunk_key_encoding; attributes; dimension_names; storage_transformers}

  let ( = ) x y =
    Shape.(x.shape = y.shape)
    && Datatype.(x.data_type = y.data_type)
    && Codecs.Chain.(x.codecs = y.codecs)
    && FillValue.(equal x.fill_value y.fill_value)
    && RegularGrid.(x.chunk_grid = y.chunk_grid)
    && ChunkKeyEncoding.(x.chunk_key_encoding = y.chunk_key_encoding)
    && Yojson.Safe.(equal x.attributes y.attributes)
    && List.equal (fun a b -> Option.equal String.equal a b) x.dimension_names y.dimension_names
    && List.equal Yojson.Safe.equal x.storage_transformers y.storage_transformers

  let codecs t = t.codecs
  let attributes t = t.attributes
  let shape t = Shape.to_list t.shape
  let dimension_names t = t.dimension_names
  let chunk_shape t = RegularGrid.chunk_shape t.chunk_grid
  let index_coord_pair t coord = RegularGrid.index_coord_pair t.chunk_grid coord
  let chunk_key t index = ChunkKeyEncoding.encode t.chunk_key_encoding index
  let chunk_indices t shape = RegularGrid.indices t.chunk_grid shape
  let encode t = Yojson.Safe.to_string (to_yojson t)
  let update_attributes t attrs = {t with attributes = attrs}
  (* FIXME: must ensure the dimensions of the array remain unchanged. *)
  let update_shape t shape = {t with shape = Shape.create shape}

  let decode s = match of_yojson (Yojson.Safe.from_string s) with
    | Error e -> raise (Parse_error e)
    | Ok m -> m

  let is_valid_kind (type a) t (kind : a Ndarray.dtype) = match kind, t.data_type with
    | Ndarray.Char, Datatype.Char
    | Ndarray.Bool, Datatype.Bool
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

  let fillvalue_of_kind (type a) t (kind : a Ndarray.dtype) : a = match kind, t.fill_value with
    | Ndarray.Char, FillValue.Char c -> c
    | Ndarray.Bool, FillValue.Bool b -> b
    | Ndarray.Int8, FillValue.Int i -> i
    | Ndarray.Uint8, FillValue.Int i -> i
    | Ndarray.Int16, FillValue.Int i -> i
    | Ndarray.Uint16, FillValue.Int i -> i
    | Ndarray.Int32, FillValue.Int i -> Int32.of_int i
    | Ndarray.Int, FillValue.Int i -> i
    | Ndarray.Int64, FillValue.Int i -> Int64.of_int i
    | Ndarray.Int64, FillValue.Intlit (_, i) -> Stdint.Uint64.to_int64 i
    | Ndarray.Uint64, FillValue.Int i -> Stdint.Uint64.of_int i 
    | Ndarray.Uint64, FillValue.Intlit (_, i) -> i 
    | Ndarray.Nativeint, FillValue.Int i -> Nativeint.of_int i
    | Ndarray.Nativeint, FillValue.Intlit (_, i) -> Stdint.Uint64.to_nativeint i
    | Ndarray.Float32, FillValue.Float f -> f 
    | Ndarray.Float64, FillValue.Float f -> f 
    | Ndarray.Complex32, FillValue.FloatComplex f -> f
    | Ndarray.Complex64, FillValue.FloatComplex f -> f
    | _ -> failwith "kind is not compatible with node's fill value."
end

module Group = struct
  type t = {zarr_format : ZarrFormat.t; node_type : NodeType.t; attributes : Yojson.Safe.t}

  let to_yojson : t -> Yojson.Safe.t = fun t ->
    let l = [("zarr_format", ZarrFormat.to_yojson t.zarr_format); ("node_type", NodeType.to_yojson t.node_type)] in
    (* optional fields.*)
    match t.attributes with
    | `Null -> `Assoc l
    | x -> `Assoc (l @ [("attributes", x)])

  let default = {zarr_format = 3; node_type = NodeType.Group; attributes = `Null}
  let encode t = Yojson.Safe.to_string (to_yojson t)
  let ( = ) x y = Yojson.Safe.(equal x.attributes y.attributes)
  let update_attributes t attrs = {t with attributes = attrs}
  let attributes t = t.attributes

  let of_yojson x =
    let open Util.Result_syntax in
    let* zarr_format = ZarrFormat.of_yojson Yojson.Safe.Util.(member "zarr_format" x) in
    let+ node_type = NodeType.Group.of_yojson Yojson.Safe.Util.(member "node_type" x) in
    {zarr_format; node_type; attributes = Yojson.Safe.Util.member "attributes" x}

  let decode s = match of_yojson (Yojson.Safe.from_string s) with
    | Error e -> raise (Parse_error e)
    | Ok m -> m

  let show t =
    let x, y = NodeType.show t.node_type, Yojson.Safe.show t.attributes in
    Format.sprintf {|"{zarr_format=%d; node_type=%s; attributes=%s}"|} t.zarr_format x y
end
