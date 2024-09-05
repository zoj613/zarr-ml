module type S = sig
  val set_char : bytes -> int -> char -> unit
  val set_int8 : bytes -> int -> int -> unit
  val set_uint8 : bytes -> int -> int -> unit
  val set_int16 : bytes -> int -> int -> unit
  val set_uint16 : bytes -> int -> int -> unit
  val set_int32 : bytes -> int -> int32 -> unit
  val set_int64 : bytes -> int -> int64 -> unit
  val set_uint64 : bytes -> int -> Stdint.uint64 -> unit
  val set_float32 : bytes -> int -> float -> unit
  val set_float64 : bytes -> int -> float -> unit
  val set_complex32 : bytes -> int -> Complex.t -> unit
  val set_complex64 : bytes -> int -> Complex.t -> unit
  val set_int : bytes -> int -> int -> unit
  val set_nativeint : bytes -> int -> nativeint -> unit

  val get_char : bytes -> int -> char
  val get_int8 : bytes -> int -> int 
  val get_uint8 : bytes -> int -> int
  val get_int16 : bytes -> int -> int
  val get_uint16 : bytes -> int -> int
  val get_int32 : bytes -> int -> int32
  val get_int64 : bytes -> int -> int64
  val get_uint64 : bytes -> int -> Stdint.uint64
  val get_float32 : bytes -> int -> float
  val get_float64 : bytes -> int -> float
  val get_complex32 : bytes -> int -> Complex.t
  val get_complex64 : bytes -> int -> Complex.t
  val get_int : bytes -> int -> int
  val get_nativeint : bytes -> int -> nativeint
end

module Little = struct
  let set_int8 = Bytes.set_int8
  let set_uint8 = Bytes.set_uint8
  let set_char buf i v = Char.code v |> set_uint8 buf i
  let set_int16 buf i v = Bytes.set_int16_le buf (2*i) v
  let set_uint16 buf i v = Bytes.set_uint16_le buf (2*i) v
  let set_int32 buf i v = Bytes.set_int32_le buf (4*i) v
  let set_int64 buf i v = Bytes.set_int64_le buf (8*i) v
  let set_uint64 buf i v = Stdint.Uint64.to_bytes_little_endian v buf (8*i)
  let set_int buf i v = Int64.of_int v |> set_int64 buf i
  let set_nativeint buf i v = Int64.of_nativeint v |> set_int64 buf i
  let set_float32 buf i v = Int32.bits_of_float v |> set_int32 buf i
  let set_float64 buf i v = Int64.bits_of_float v |> set_int64 buf i
  let set_complex32 buf i Complex.{re; im} = 
    Int32.bits_of_float re |> Bytes.set_int32_le buf (8*i);
    Int32.bits_of_float im |> Bytes.set_int32_le buf (8*i + 4)
  let set_complex64 buf i Complex.{re; im} = 
    Int64.bits_of_float re |> Bytes.set_int64_le buf (16*i);
    Int64.bits_of_float im |> Bytes.set_int64_le buf (16*i + 8)

  let get_int8 = Bytes.get_int8
  let get_uint8 = Bytes.get_uint8
  let get_char buf i = get_uint8 buf i |> Char.chr
  let get_int16 = Bytes.get_int16_le
  let get_uint16 = Bytes.get_uint16_le
  let get_int32 = Bytes.get_int32_le
  let get_int64 = Bytes.get_int64_le
  let get_uint64 = Stdint.Uint64.of_bytes_little_endian
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
  let set_int8 = Bytes.set_int8
  let set_uint8 = Bytes.set_uint8
  let set_char buf i v = Char.code v |> set_uint8 buf i
  let set_int16 buf i v = Bytes.set_int16_be buf (i * 2) v
  let set_uint16 buf i v = Bytes.set_uint16_be buf (i * 2) v
  let set_int32 buf i v = Bytes.set_int32_be buf (i * 4) v
  let set_int64 buf i v = Bytes.set_int64_be buf (i * 8) v
  let set_uint64 buf i v = Stdint.Uint64.to_bytes_big_endian v buf (i * 8)
  let set_int buf i v = Int64.of_int v |> set_int64 buf i
  let set_nativeint buf i v = Int64.of_nativeint v |> set_int64 buf i
  let set_float32 buf i v = Int32.bits_of_float v |> set_int32 buf i
  let set_float64 buf i v = Int64.bits_of_float v |> set_int64 buf i
  let set_complex32 buf i Complex.{re; im} = 
    Int32.bits_of_float re |> Bytes.set_int32_be buf (8*i);
    Int32.bits_of_float im |> Bytes.set_int32_be buf (8*i + 4)
  let set_complex64 buf i Complex.{re; im} = 
    Int64.bits_of_float re |> Bytes.set_int64_be buf (16*i);
    Int64.bits_of_float im |> Bytes.set_int64_be buf (16*i + 8)

  let get_int8 = Bytes.get_int8
  let get_uint8 = Bytes.get_uint8
  let get_char buf i = get_uint8 buf i |> Char.chr
  let get_int16 = Bytes.get_int16_be
  let get_uint16 = Bytes.get_uint16_be
  let get_int32 = Bytes.get_int32_be
  let get_int64 = Bytes.get_int64_be
  let get_uint64 = Stdint.Uint64.of_bytes_big_endian
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
