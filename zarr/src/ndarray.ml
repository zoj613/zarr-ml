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

type 'a t =
  {shape : int array
  ;strides : int array
  ;dtype : 'a dtype
  ;data : 'a array}

let dtype_size : type a. a dtype -> int = function
  | Char -> 1
  | Int8 -> 1
  | Uint8 -> 1
  | Int16 -> 2
  | Uint16 -> 2
  | Int32 -> 4
  | Int64 -> 8
  | Uint64 -> 8
  | Float32 -> 4
  | Float64 -> 8
  | Complex32 -> 8
  | Complex64 -> 16
  | Int -> Sys.word_size / 8
  | Nativeint -> Sys.word_size / 8

let cumprod x start stop =
  let acc = ref 1 in
  for i = start to stop do acc := !acc * x.(i) done; !acc

(*strides[k] = [cumulative_product with start=k+1 end=n-1] of shape *)
let make_strides shape =
  let n = Array.length shape - 1 in
  Array.init (n + 1) (fun i -> cumprod shape (i + 1) n)

let create dtype shape fv =
  {shape
  ;dtype
  ;strides = make_strides shape
  ;data = Array.make (Util.prod shape) fv}

let init dtype shape f = 
  {shape
  ;dtype
  ;strides = make_strides shape
  ;data = Array.init (Util.prod shape) f}

let data_type t = t.dtype

let size t = Util.prod t.shape

let ndims t = Array.length t.shape

let shape t = t.shape

let byte_size t = size t * dtype_size t.dtype 

let to_array t = t.data

let of_array dtype shape xs =
  {shape; dtype; strides = make_strides shape; data = xs}

(* 1d index of coord [i0; ...; in] is SUM(i0 * strides[0] + ... + in * strides[n-1] *)
let coord_to_index i s =
  Array.fold_left (fun a (x, y) -> Int.add a (x * y)) 0 @@ Array.combine i s

let get t i = t.data.(coord_to_index i t.strides)

let set t i x = t.data.(coord_to_index i t.strides) <- x

let iteri f t = Array.iteri f t.data

let fill t v = Array.iteri (fun i _ -> t.data.(i) <- v) t.data

let map f t = {t with data = Array.map f t.data}

let iter f t = Array.iter f t.data

let equal x y =
  x.data = y.data
  && x.shape = y.shape
  && x.dtype = y.dtype
  && x.strides = y.strides

(* validation for [axis] is done at the boundaries of the system and thus doing
   so inside this function would be redundant work. Also, the output array
   shares internal data with the input. Since this function is only ever
   used when serializing/deserializing an Ndarray.t type then this should not
   be an issue since the input array is never used again after it is transposed. *)
let transpose ?axis x =
  let n = ndims x in
  let p = Option.fold ~none:(Array.init n (fun i -> n - 1 - i)) ~some:Fun.id axis in
  let shape = Array.map (fun i -> x.shape.(i)) p in
  {x with shape; strides = make_strides shape}
