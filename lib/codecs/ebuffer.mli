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
end

module Little : sig include S end

module Big : sig include S end
