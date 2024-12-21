module type S = sig
  val set_char : bytes -> int -> char -> unit
  val set_bool : bytes -> int -> bool -> unit
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
  val get_bool : bytes -> int -> bool
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

module Little : sig include S end

module Big : sig include S end
