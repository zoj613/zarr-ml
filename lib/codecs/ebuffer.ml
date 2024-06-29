module type S = sig
  val contents : Buffer.t -> string
  val add_char : Buffer.t -> char -> unit
  val add_int8 : Buffer.t -> int -> unit
  val add_uint8 : Buffer.t -> int -> unit
  val add_int16 : Buffer.t -> int -> unit
  val add_uint16 : Buffer.t -> int -> unit
  val add_int32 : Buffer.t -> int32 -> unit
  val add_int64 : Buffer.t -> int64 -> unit
  val add_float32 : Buffer.t -> float -> unit
  val add_float64 : Buffer.t -> float -> unit
  val add_complex32 : Buffer.t -> Complex.t -> unit
  val add_complex64 : Buffer.t -> Complex.t -> unit
  val add_int : Buffer.t -> int -> unit
  val add_nativeint : Buffer.t -> nativeint -> unit

  val get_char : string -> int -> char
  val get_int8 : string -> int -> int 
  val get_uint8 : string -> int -> int
  val get_int16 : string -> int -> int
  val get_uint16 : string -> int -> int
  val get_int32 : string -> int -> int32
  val get_int64 : string -> int -> int64
  val get_float32 : string -> int -> float
  val get_float64 : string -> int -> float
  val get_complex32 : string -> int -> Complex.t
  val get_complex64 : string -> int -> Complex.t
  val get_int : string -> int -> int
  val get_nativeint : string -> int -> nativeint
end

module Little = struct
  let contents = Buffer.contents
  let add_int8 = Buffer.add_int8
  let add_char buf v = Char.code v |> add_int8 buf
  let add_uint8 = Buffer.add_uint8
  let add_int16 = Buffer.add_int16_le
  let add_uint16 = Buffer.add_uint16_le
  let add_int32 = Buffer.add_int32_le
  let add_int64 = Buffer.add_int64_le
  let add_int buf v = Int64.of_int v |> add_int64 buf
  let add_nativeint buf v = Int64.of_nativeint v |> add_int64 buf
  let add_float32 buf v = Int32.bits_of_float v |> add_int32 buf
  let add_float64 buf v = Int64.bits_of_float v |> add_int64 buf
  let add_complex32 buf Complex.{re; im} = 
    Int32.bits_of_float re |> add_int32 buf;
    Int32.bits_of_float im |> add_int32 buf
  let add_complex64 buf Complex.{re; im} = 
    Int64.bits_of_float re |> add_int64 buf;
    Int64.bits_of_float im |> add_int64 buf

  let get_int8 = String.get_int8
  let get_char buf i = get_int8 buf i |> Char.chr
  let get_uint8 = String.get_uint8
  let get_int16 = String.get_int16_le
  let get_uint16 = String.get_uint16_le
  let get_int32 = String.get_int32_le
  let get_int64 = String.get_int64_le
  let get_int buf i = get_int64 buf i |> Int64.to_int
  let get_nativeint buf i = get_int64 buf i |> Int64.to_nativeint
  let get_float32 buf i = get_int32 buf i |> Int32.float_of_bits
  let get_float64 buf i = get_int64 buf i |> Int64.float_of_bits
  let get_complex32 buf i =
    let re, im = get_float32 buf i, get_float32 buf (i + 4) in
    Complex.{re; im}
  let get_complex64 buf i =
    let re, im = get_float64 buf i, get_float64 buf (i + 8) in
    Complex.{re; im}
end

module Big = struct
  let contents = Buffer.contents
  let add_int8 = Buffer.add_int8
  let add_char buf v = Char.code v |> add_int8 buf
  let add_uint8 = Buffer.add_uint8
  let add_int16 = Buffer.add_int16_be
  let add_uint16 = Buffer.add_uint16_be
  let add_int32 = Buffer.add_int32_be
  let add_int64 = Buffer.add_int64_be
  let add_int buf v = Int64.of_int v |> add_int64 buf
  let add_nativeint buf v = Int64.of_nativeint v |> add_int64 buf
  let add_float32 buf v = Int32.bits_of_float v |> Buffer.add_int32_be buf
  let add_float64 buf v = Int64.bits_of_float v |> Buffer.add_int64_be buf
  let add_complex32 buf Complex.{re; im} = 
    Int32.bits_of_float re |> Buffer.add_int32_be buf;
    Int32.bits_of_float im |> Buffer.add_int32_be buf
  let add_complex64 buf Complex.{re; im} = 
    Int64.bits_of_float re |> Buffer.add_int64_be buf;
    Int64.bits_of_float im |> Buffer.add_int64_be buf

  let get_int8 = String.get_int8
  let get_char buf i = get_int8 buf i |> Char.chr
  let get_uint8 = String.get_uint8
  let get_int16 = String.get_int16_be
  let get_uint16 = String.get_uint16_be
  let get_int32 = String.get_int32_be
  let get_int64 = String.get_int64_be
  let get_int buf i = get_int64 buf i |> Int64.to_int
  let get_nativeint buf i = get_int64 buf i |> Int64.to_nativeint
  let get_float32 buf i = get_int32 buf i |> Int32.float_of_bits
  let get_float64 buf i = get_int64 buf i |> Int64.float_of_bits
  let get_complex32 buf i =
    let re, im = get_float32 buf i, get_float32 buf (i + 4) in
    Complex.{re; im}
  let get_complex64 buf i =
    let re, im = get_float64 buf i, get_float64 buf (i + 8) in
    Complex.{re; im}
end
