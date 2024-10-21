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

type 'a t =
  {shape : int array
  ;strides : int array
  ;dtype : 'a dtype
  ;data : 'a array}

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

(* This snippet is adapted from the Owl project.

   The MIT License (MIT)
   Copyright (c) 2016-2022 Liang Wang liang@ocaml.xyz *)
let index_to_coord ~strides i j =
  j.(0) <- i / strides.(0);
  for k = 1 to Array.length strides - 1 do
    j.(k) <- i mod strides.(k - 1) / strides.(k)
  done

let get t i = t.data.(coord_to_index i t.strides)

let set t i x = t.data.(coord_to_index i t.strides) <- x

let iteri f t = Array.iteri f t.data

let fill t v = Array.iteri (fun i _ -> t.data.(i) <- v) t.data

let map f t = {t with data = Array.map f t.data}

let iter f t = Array.iter f t.data

module B = Bigarray

let to_bigarray :
  type a b. a t -> (a, b) B.kind -> (a, b, B.c_layout) B.Genarray.t
  = fun x kind ->
  let f k =
    B.Genarray.init k C_layout x.shape @@ fun c ->
    x.data.(coord_to_index c x.strides) in
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
  let shape = B.Genarray.dims x' in
  let coord = Array.make (B.Genarray.num_dims x') 0 in
  let strides = make_strides shape in
  let value_at_index i =
    index_to_coord ~strides i coord;
    B.Genarray.get x' coord
  in
  let f d = init d shape value_at_index in
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
let transpose ?axes x =
  let n = ndims x in
  let p = Option.fold ~none:(Array.init n (fun i -> n - 1 - i)) ~some:Fun.id axes in
  let shape = Array.map (fun i -> x.shape.(i)) p in
  let x' = {x with shape; strides = make_strides shape; data = Array.copy x.data} in
  let c = Array.make n 0 and c' = Array.make n 0 in
  (* Project a 1d-indexed value of the input ndarray into its corresponding
     n-dimensional index/coordinate of the transposed ndarray according to the
     permutation described by [p].*)
  let project_1d_to_nd i a =
    index_to_coord ~strides:x.strides i c;
    Array.iteri (fun j b -> c'.(j) <- c.(b)) p; 
    set x' c' a
  in
  iteri project_1d_to_nd x;
  x'

(* The [index] type definition as well as functions tagged with [@coverage off]
  in this Indexing module were directly copied from the Owl project to emulate
  its logic for munipulating slices. The code is licenced under the MIT license 
  and can be found at: https://github.com/owlbarn/owl

  The MIT License (MIT)
  Copyright (c) 2016-2022 Liang Wang liang@ocaml.xyz *)
module Indexing = struct
  type index =
    | I of int
    | L of int array
    | R of int array

  (* this is copied from the Owl project so we skip testing it. *)
  let[@coverage off] check_slice_definition axis shp =
    let axis_len = Array.length axis in
    let shp_len = Array.length shp in
    assert (axis_len <= shp_len);
    (* add missing definition on higher dimensions *)
    let axis =
      if axis_len < shp_len
      then (
        let suffix = Array.make (shp_len - axis_len) (R [||]) in
        Array.append axis suffix)
      else axis
    in
    (* re-format slice definition, note I_ will be replaced with L_ *)
    Array.map2
      (fun i n ->
        match i with
        | I x ->
          let x = if x >= 0 then x else n + x in
          assert (x < n);
          R [| x; x; 1 |]
        | L x ->
          let is_cont = ref true in
          if Array.length x <> n then is_cont := false;
          let x =
            Array.mapi
              (fun i j ->
                let j = if j >= 0 then j else n + j in
                assert (j < n);
                if i <> j then is_cont := false;
                j)
              x
          in
          if !is_cont = true then R [| 0; n - 1; 1 |] else L x
        | R x ->
          (match Array.length x with
          | 0 -> R [| 0; n - 1; 1 |]
          | 1 ->
            let a = if x.(0) >= 0 then x.(0) else n + x.(0) in
            assert (a < n);
            R [| a; a; 1 |]
          | 2 ->
            let a = if x.(0) >= 0 then x.(0) else n + x.(0) in
            let b = if x.(1) >= 0 then x.(1) else n + x.(1) in
            let c = if a <= b then 1 else -1 in
            assert (not (a >= n || b >= n));
            R [| a; b; c |]
          | 3 ->
            let a = if x.(0) >= 0 then x.(0) else n + x.(0) in
            let b = if x.(1) >= 0 then x.(1) else n + x.(1) in
            let c = x.(2) in
            assert (not (a >= n || b >= n || c = 0));
            assert (not ((a < b && c < 0) || (a > b && c > 0)));
            R [| a; b; c |]
          | _ -> failwith "check_slice_definition: error"))
      axis
      shp

  (* this was opied from the Owl project so we skip testing it. *)
  let[@coverage off] calc_slice_shape axis =
    Array.map
      (function
      | I _x -> 1 (* never reached *)
      | L x -> Array.length x
      | R x -> abs ((x.(1) - x.(0)) / x.(2)) + 1) axis

  let rec cartesian_prod :
    int list list -> int list list = function
    | [] -> [[]]
    | x :: xs ->
      List.concat_map (fun i ->
        List.map (List.cons i) (cartesian_prod xs)) x

  let range ~step start stop =
    List.of_seq @@ if step > 0 then
      Seq.unfold (function
        | x when x > stop -> None
        | x -> Some (x, x + step)) start
    else
      let start, stop = stop, start in
      Seq.unfold (function
        | x when x < start -> None
        | x -> Some (x, x + step)) stop

  (* get indices from a reformated slice *)
  let indices_of_slice = function
    | R [|start; stop; step|] -> range ~step start stop
    | L l -> Array.to_list l
    (* this is added for exhaustiveness but is never reached since
      a reformatted slice replaces a I index with an R index.*)
    | _ -> failwith "Invalid slice index."

  let coords_of_slice slice shape =
    (Array.map indices_of_slice @@ check_slice_definition slice shape)
    |> Array.to_list
    |> cartesian_prod
    |> List.map Array.of_list
    |> Array.of_list

  let slice_of_coords = function
    | [] -> [||]
    | x :: _ as xs ->
      let module IntSet = Set.Make(Int) in
      let ndims = Array.length x in
      let indices = Array.make ndims IntSet.empty in
      Array.map (fun x -> L (IntSet.elements x |> Array.of_list)) @@
      List.fold_right (fun x acc ->
        Array.iteri (fun i y ->
          if IntSet.mem y acc.(i) then ()
          else acc.(i) <- IntSet.add y acc.(i)) x; acc) xs indices

  let slice_shape slice array_shape =
    calc_slice_shape @@ check_slice_definition slice array_shape
end
