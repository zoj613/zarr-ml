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
]
