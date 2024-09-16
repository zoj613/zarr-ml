open OUnit2

module M = Zarr.Ndarray

let run_test :
  type a. a Zarr.Codecs.array_repr -> a -> int -> unit = fun repr fv is ->
  let x = M.create repr.kind repr.shape fv in

  assert_equal repr.shape (M.shape x);
  let num_elt = Array.fold_left Int.mul 1 repr.shape in
  assert_equal (num_elt * is) (M.byte_size x);
  assert_equal num_elt (M.size x);
  assert_equal is (M.dtype_size @@ M.data_type x);
  assert_equal (Array.length repr.shape) (M.ndims x);

  let y = M.init repr.kind repr.shape (Fun.const fv) in
  assert_equal x y;
  M.fill y fv;
  assert_equal x y;
  assert_equal fv (M.get x [|0; 0; 0|]);
  M.set x [|0; 0; 0|] fv;
  assert_bool "" @@ M.equal x y;
  M.iteri (fun _ v -> ignore v) x

let tests = [
"test char ndarray" >:: (fun _ ->
  let shape = [|2; 5; 3|] in
  run_test {shape; kind = M.Char} '?' 1;

  run_test {shape; kind = M.Bool} false 1;

  run_test {shape; kind = M.Int8} 0 1;

  run_test {shape; kind = M.Uint8} 0 1;

  run_test {shape; kind = M.Int16} 0 2;

  run_test {shape; kind = M.Uint16} 0 2;

  run_test {shape; kind = M.Int32} Int32.max_int 4;

  run_test {shape; kind = M.Int64} Int64.max_int 8;

  run_test {shape; kind = M.Float32} Float.neg_infinity 4;

  run_test {shape; kind = M.Float64} Float.neg_infinity 8;

  run_test {shape; kind = M.Complex32} Complex.zero 8;

  run_test {shape; kind = M.Complex64} Complex.zero 16;

  run_test {shape; kind = M.Int} Int.max_int @@ Sys.word_size / 8;

  run_test {shape; kind = M.Nativeint} Nativeint.max_int @@ Sys.word_size / 8
)
;
"test map, iter and fold" >:: (fun _ ->
  let shape = [|2; 5; 3|] in
  let x = M.create Int32 shape 0l in
  let x' = M.map (Int32.add 1l) x in
  assert_equal 1l (M.get x' [|0;0;0|]);

  let x = M.create Char [|4|] '?' in
  let buf = Buffer.create @@ M.byte_size x in
  M.iter (Buffer.add_char buf) x;
  assert_equal ~printer:Fun.id "????" (Buffer.contents buf);
)
;
"test interop with bigarrays" >:: (fun _ ->
  let s = [|2; 5; 3|] in
  let module B = Bigarray in

  let convert_to :
    type a b. a M.dtype -> (a, b) B.kind -> a -> unit
    = fun fromdtype todtype fv ->
    let x = M.create fromdtype s fv in
    let y = M.to_bigarray x todtype in
    assert_equal s (B.Genarray.dims y);
    assert_equal fv (B.Genarray.get y [|0; 0; 0|]);  
    assert_equal B.c_layout (B.Genarray.layout y)
  in
  convert_to M.Char B.Char '?';
  convert_to M.Int8 B.Int8_signed 127;
  convert_to M.Uint8 B.Int8_unsigned 255;
  convert_to M.Int16 B.Int16_signed 32767;
  convert_to M.Uint16 B.Int16_unsigned 32767;
  convert_to M.Int32 B.Int32 Int32.max_int;
  convert_to M.Int64 B.Int64 Int64.max_int;
  convert_to M.Float32 B.Float32 Float.neg_infinity;
  convert_to M.Float64 B.Float64 Float.infinity;
  convert_to M.Complex32 B.Complex32 Complex.one;
  convert_to M.Complex64 B.Complex64 Complex.zero;
  convert_to M.Int B.Int Int.max_int;
  convert_to M.Nativeint B.Nativeint Nativeint.max_int;

  let showarray = [%show: int array] in

  let convert_from :
    type a b c. (a, b, c) B.Genarray.t -> a M.dtype -> unit = fun x dtype ->
    let y = M.of_bigarray x in
    assert_equal dtype (M.data_type y);
    assert_equal
      ~printer:showarray
      (Array.of_list @@ List.rev @@ Array.to_list @@ B.Genarray.dims x)
      (M.shape y);
    assert_equal (B.Genarray.get x [|1; 1; 1|]) (M.get y [|0; 0; 0|])
  in
  convert_from (B.Genarray.create Char Fortran_layout s) M.Char;
  convert_from (B.Genarray.create Int8_signed Fortran_layout s) M.Int8;
  convert_from (B.Genarray.create Int8_unsigned Fortran_layout s) M.Uint8;
  convert_from (B.Genarray.create Int16_signed Fortran_layout s) M.Int16;
  convert_from (B.Genarray.create Int16_unsigned Fortran_layout s) M.Uint16;
  convert_from (B.Genarray.create Int32 Fortran_layout s) M.Int32;
  convert_from (B.Genarray.create Int64 Fortran_layout s) M.Int64;
  convert_from (B.Genarray.create Float32 Fortran_layout s) M.Float32;
  convert_from (B.Genarray.create Float64 Fortran_layout s) M.Float64;
  convert_from (B.Genarray.create Complex32 Fortran_layout s) M.Complex32;
  convert_from (B.Genarray.create Complex64 Fortran_layout s) M.Complex64;
  convert_from (B.Genarray.create Int Fortran_layout s) M.Int;
  convert_from (B.Genarray.create Nativeint Fortran_layout s) M.Nativeint
)
]
