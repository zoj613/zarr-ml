type _ dtype =
  | Char : char dtype
  | Bool : bool dtype
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

let dtype_size : type a. a dtype -> int = function
  | Char -> 1
  | Bool -> 1
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

let prod x = List.fold_left Int.mul 1 x

let cumprod x start stop =
  let acc = ref 1 in
  for i = start to stop do acc := !acc * (List.nth x i) done; !acc

(*strides[k] = [cumulative_product with start=k+1 end=n-1] of shape *)
let make_strides shape =
  let n = List.length shape - 1 in
  Array.init (n + 1) (fun i -> cumprod shape (i + 1) n)

type 'a t = {shape : int list; strides : int array; dtype : 'a dtype; data : 'a array}
let equal x y = x.data = y.data && x.shape = y.shape && x.dtype = y.dtype && x.strides = y.strides
(* 1d index of coord [i0; ...; in] is SUM(i0 * strides[0] + ... + in * strides[n-1] *)
let coord_to_index i s = Array.fold_left (fun a (x, y) -> Int.add a (x * y)) 0 @@ Array.combine i s
let coord_to_index' x s = let acc = ref 0 in List.iteri (fun i v -> acc := !acc + v * s.(i)) x; !acc
let create dtype shape fv = {shape; dtype; strides = make_strides shape; data = Array.make (prod shape) fv}
let init dtype shape f = {shape; dtype; strides = make_strides shape; data = Array.init (prod shape) f}
let of_array dtype shape xs = {shape; dtype; strides = make_strides shape; data = xs}
let data_type t = t.dtype
let size t = prod t.shape
let ndims t = List.length t.shape
let get t i = t.data.(coord_to_index' i t.strides)
let set t i x = t.data.(coord_to_index' i t.strides) <- x
let set' t i x = t.data.(coord_to_index i t.strides) <- x
let fill t v = Array.iteri (fun i _ -> t.data.(i) <- v) t.data
let map f t = {t with data = Array.map f t.data}
let iteri f t = Array.iteri f t.data
let iter f t = Array.iter f t.data
let byte_size t = size t * dtype_size t.dtype 
let to_array t = t.data
let shape t = t.shape

(* This snippet is adapted from the Owl project.

   The MIT License (MIT)
   Copyright (c) 2016-2022 Liang Wang liang@ocaml.xyz *)
let index_to_coord ~strides i j =
  j.(0) <- i / strides.(0);
  for k = 1 to Array.length strides - 1 do
    j.(k) <- i mod strides.(k - 1) / strides.(k)
  done

module B = Bigarray

let to_bigarray :
  type a b. a t -> (a, b) B.kind -> (a, b, B.c_layout) B.Genarray.t
  = fun x kind ->
  let initialize ~x c = x.data.(coord_to_index c x.strides) in
  let shape = Array.of_list x.shape in
  let f k = B.Genarray.init k C_layout shape (initialize ~x) in
  match[@warning "-8"] kind with
  | B.Char as k -> f k
  | B.Int8_signed as k -> f k
  | B.Int8_unsigned as k -> f k
  | B.Int16_signed as k -> f k
  | B.Int16_unsigned as k -> f k
  | B.Int32 as k -> f k
  | B.Int64 as k -> f k
  | B.Float32 as k -> f k
  | B.Float64 as k -> f k
  | B.Nativeint as k -> f k
  | B.Int as k -> f k
  | B.Complex32 as k -> f k
  | B.Complex64 as k -> f k

let of_bigarray :
  type a b c. (a, b, c) B.Genarray.t -> a t = fun x ->
  let x' = B.Genarray.change_layout x C_layout in
  let shape = B.Genarray.dims x' |> Array.to_list in
  let coord = Array.make (B.Genarray.num_dims x') 0 in
  let strides = make_strides shape in
  let initialize ~strides ~coord ~x' i =
    index_to_coord ~strides i coord;
    B.Genarray.get x' coord
  in
  let f d = init d shape (initialize ~strides ~coord ~x') in
  match[@warning "-8"] B.Genarray.kind x with
  | B.Char -> f Char
  | B.Int8_signed -> f Int8
  | B.Int8_unsigned -> f Uint8
  | B.Int16_signed -> f Int16
  | B.Int16_unsigned -> f Uint16
  | B.Int32 -> f Int32
  | B.Int64 -> f Int64
  | B.Float32 -> f Float32
  | B.Float64 -> f Float64
  | B.Nativeint -> f Nativeint
  | B.Int -> f Int
  | B.Complex32 -> f Complex32
  | B.Complex64 -> f Complex64

(* validation for [axis] is done at the boundaries of the system and thus doing
   so inside this function would be redundant work. Also, the output array
   shares internal data with the input. Since this function is only ever
   used when serializing/deserializing an Ndarray.t type then this should not
   be an issue since the input array is never used again after it is transposed. *)
let transpose ?axes x =
  let n = ndims x in
  let p = Option.fold ~none:(List.init n (fun i -> n - 1 - i)) ~some:Fun.id axes in
  let shape = List.map (fun i -> List.nth x.shape i) p in
  let x' = {x with shape; strides = make_strides shape; data = Array.copy x.data} in
  let c = Array.make n 0 and c' = Array.make n 0 in
  (* Project a 1d-indexed value of the input ndarray into its corresponding
     n-dimensional index/coordinate of the transposed ndarray according to the
     permutation described by [p].*)
  let project_1d_to_nd i a =
    index_to_coord ~strides:x.strides i c;
    List.iteri (fun j b -> c'.(j) <- c.(b)) p; 
    set' x' c' a
  in
  iteri project_1d_to_nd x;
  x'

(* The [index] type definition as well as functions tagged with [@coverage off]
  in this Indexing module were directly copied and modified from the Owl project
  to emulate its logic for munipulating slices. The code is licenced under the
  MIT license and can be found at: https://github.com/owlbarn/owl

  The MIT License (MIT)
  Copyright (c) 2016-2022 Liang Wang liang@ocaml.xyz *)
module Indexing = struct
  type index =
    | F
    | I of int
    | T of int
    | L of int list
    | R of int * int
    | R' of int * int * int

  (* internal restricted representation of index type *)
  type index' = L of int list | R' of int * int * int
  type t = index' list

  type error = [ `Invalid_array_slice | `Invalid_data_type ]

  (* this is copied from the Owl project so we skip testing it. *)
  let[@coverage off] check_slice_definition axis shp =
    let open Util.Result_syntax in
    let axis_len = List.length axis in
    let shp_len = List.length shp in
    if not (axis_len <= shp_len) then Error `Invalid_array_slice else
    (* add missing definition on higher dimensions *)
    let axis = if axis_len < shp_len then axis @ List.init (shp_len - axis_len) (fun _ -> F) else axis in
    (* re-format slice definition, note I_ will be replaced with L_ *)
    List.fold_right2
      (fun i n acc ->
      let* xs = acc in match i with
      | I x ->
        let x = if x >= 0 then x else n + x in
        if not (x < n) then Error `Invalid_array_slice else
        Ok (R' (x, x, 1) :: xs)
      | L x ->
        let is_cont = ref true in
        let lx = List.length x in
        if lx <> n then is_cont := false;
        let idx = List.init lx Fun.id in
        let* x =
          List.fold_right2
            (fun i j k ->
              let* acc' = k in
              let j = if j >= 0 then j else n + j in
              if not (j < n) then Error `Invalid_array_slice else
              (if i <> j then is_cont := false; Ok (j :: acc')))
            idx x (Ok [])
        in
        if !is_cont = true then Ok (R' (0, n-1, 1) :: xs) else Ok (L x :: xs)
      | F -> Ok (R' (0, n - 1, 1) :: xs)
      | T x ->
        let a = if x >= 0 then x else n + x in
        if not (a < n) then Error `Invalid_array_slice else Ok (R' (a, a, 1) :: xs)
      | R (x, y) ->
        let a = if x >= 0 then x else n + x in
        let b = if y >= 0 then y else n + y in
        let c = if a <= b then 1 else -1 in
        assert (not (a >= n || b >= n));
        if (a >= n || b >= n) then Error `Invalid_array_slice else Ok (R' (a, b, c) :: xs)
      | R' (x, y, c) ->
        let a = if x >= 0 then x else n + x in
        let b = if y >= 0 then y else n + y in
        if (a >= n || b >= n || c = 0) || ((a < b && c < 0) || (a > b && c > 0))
        then Error `Invalid_array_slice else Ok (R' (a, b, c) :: xs))
      axis shp (Ok [])

  (* this was opied from the Owl project so we skip testing it. *)
  let[@coverage off] calc_slice_shape axis =
    let f = function
      | L x -> List.length x
      | R' (x, y, z) -> abs ((y - x) / z) + 1
    in
    List.map f axis

  let create = check_slice_definition
  let slice_shape slice = calc_slice_shape slice

  let range ~step start stop =
    let rec aux ~step ~stop acc = function
      | x when (step < 0 && x < stop) || (step > 0 && x > stop) -> List.rev acc
      | x -> aux ~step ~stop (x :: acc) (x + step)
    in
    aux ~step ~stop [] start
       
  let coords_of_slice slice =
    let indices_of_slice = function
      | R' (start, stop, step) -> range ~step start stop
      | L x -> x
    in
    Util.cartesian_prod (List.map indices_of_slice slice)
end
