open OUnit2
open Zarr
open Zarr.Indexing


let tests = [
"coords from slice" >:: (fun _ -> 
  let shape = [10; 10; 10] in
  let indices = [L [0; 9; 5]; I 1; R' (9, 3, -3)] in
  let slice = Result.get_ok (Indexing.create indices shape) in
  let expected = [[0; 1; 9]; [0; 1; 6]; [0; 1; 3]; [9; 1; 9]; [9; 1; 6]; [9; 1; 3] ;[5; 1; 9]; [5; 1; 6]; [5; 1; 3]] in
  assert_equal ~printer:[%show: int list list] expected @@ Indexing.coords_of_slice slice;

  (* test using an empty slice translates to selection the whole array. *)
  let slice = Result.get_ok (Indexing.create [] [2; 2]) in
  assert_equal [[0; 0]; [0; 1]; [1; 0]; [1; 1]] (Indexing.coords_of_slice slice);

  (* test missing definition on higher dimensions *)
  let shape = [3; 3; 3] in
  let expected = [[2; 0; 0]; [2; 0; 1]; [2; 0; 2]] in
  let indices = [I 2; I 0] in
  let slice = Result.get_ok (Indexing.create indices shape) in
  assert_equal expected (Indexing.coords_of_slice slice);
  (* test negative I value *)
  let expected = [[2; 2; 0]; [2; 2; 1]; [2; 2; 2]] in
  let indices = [I 2; I (-1)] in
  let slice = Result.get_ok (Indexing.create indices shape) in
  assert_equal expected (Indexing.coords_of_slice slice);

  let indices = [R (-1, 2); L [-1]; L [0; 0; 0]] in
  let slice = Result.get_ok (Indexing.create indices shape) in
  let expected = [[2; 2; 0]; [2; 2; 0]; [2; 2; 0]] in
  assert_equal expected (Indexing.coords_of_slice slice);

  let indices = [R (0, -2); T 1; T (-1)] in
  let slice = Result.get_ok (Indexing.create indices shape) in
  let expected = [[0; 1; 2]; [1; 1; 2]] in
  assert_equal expected (Indexing.coords_of_slice slice);

  let indices = [R (1, 0); T 1; T (-1)] in
  let slice = Result.get_ok (Indexing.create indices shape) in
  let expected = [[1; 1; 2]; [0; 1; 2]] in
  assert_equal expected (Indexing.coords_of_slice slice);

  let indices = [I 2; I (-1); R' (-1, -1, 1)] in
  let slice = Result.get_ok (Indexing.create indices shape) in
  let expected = [[2; 2; 2]] in
  assert_equal expected (Indexing.coords_of_slice slice)
)
;
"compute slice shape" >:: (fun _ ->
  let shape = [10; 10; 10] in
  let indices = [L [0; 9; 5]; I 1; R' (2, 9, 1)] in
  let slice = Result.get_ok (Indexing.create indices shape) in
  assert_equal [3; 1; 8] (Indexing.slice_shape slice);
  let slice = Result.get_ok (Indexing.create [] shape) in
  assert_equal shape (Indexing.slice_shape slice))
]
