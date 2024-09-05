type _ dtype =
  | Char : char dtype
  | Int8 : int dtype
  | Uint8 : int dtype
  | Int16 : int dtype
  | Uint16 : int dtype
  | Int32 : int32 dtype
  | Int64 : int64 dtype
  | Uint64 : Stdint.uint64 dtype
  | Float32 : float dtype
  | Float64 : float dtype
  | Complex32 : Complex.t dtype
  | Complex64 : Complex.t dtype
  | Int : int dtype
  | Nativeint : nativeint dtype

type 'a t

val dtype_size : 'a dtype -> int

val create : 'a dtype -> int array -> 'a -> 'a t

val init : 'a dtype -> int array -> (int -> 'a) -> 'a t

val data_type : 'a t -> 'a dtype

val size : 'a t -> int

val ndims : 'a t -> int

val shape : 'a t -> int array

val byte_size : 'a t -> int

val to_array : 'a t -> 'a array 

val of_array : 'a dtype -> int array -> 'a array -> 'a t

val get : 'a t -> int array -> 'a

val set : 'a t -> int array -> 'a -> unit

val iteri : (int -> 'a -> unit) -> 'a t -> unit

val fill : 'a t -> 'a -> unit

val map : ('a -> 'a) -> 'a t -> 'a t

val iter : ('a -> unit) -> 'a t -> unit

val equal : 'a t -> 'a t -> bool

val transpose : ?axis:int array -> 'a t -> 'a t
