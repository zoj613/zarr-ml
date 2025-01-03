open OUnit2
open Zarr
open Zarr.Indexing


let tests = [

"slice from coords" >:: (fun _ ->
  (* slice_of_coords should be duplicate coord-aware *)
  let coords = [[0; 1; 2; 3]; [9; 8; 7; 6]; [9; 8; 7; 6]; [5; 4; 3; 2]] in
  let expected = [L [0; 5; 9]; L [1; 4; 8]; L [2; 3; 7]; L [2; 3; 6]] in
  assert_equal expected @@ Indexing.slice_of_coords coords;
  assert_equal [] @@ Indexing.slice_of_coords [])
; 
"coords from slice" >:: (fun _ -> 
  let shape = [10; 10; 10] in
  let slice = [L [0; 9; 5]; I 1; R' (9, 3, -3)] in
  let expected = [[0; 1; 9]; [0; 1; 6]; [0; 1; 3]; [9; 1; 9]; [9; 1; 6]; [9; 1; 3] ;[5; 1; 9]; [5; 1; 6]; [5; 1; 3]] in
  assert_equal ~printer:[%show: int list list] expected @@ Indexing.coords_of_slice slice shape;

  (* test using an empty slice translates to selection the whole array. *)
  assert_equal [[0; 0]; [0; 1]; [1; 0]; [1; 1]] (Indexing.coords_of_slice [] [2; 2]);

  (* test missing definition on higher dimensions *)
  let shape = [3; 3; 3] in
  let expected = [[2; 0; 0]; [2; 0; 1]; [2; 0; 2]] in
  let slice = [I 2; I 0] in
  assert_equal expected @@ Indexing.coords_of_slice slice shape;
  (* test negative I value *)
  let expected = [[2; 2; 0]; [2; 2; 1]; [2; 2; 2]] in
  let slice = [I 2; I (-1)] in
  assert_equal expected @@ Indexing.coords_of_slice slice shape;
  let slice = [R (-1, 2); L [-1]; L [0; 0; 0]] in
  let expected = [[2; 2; 0]; [2; 2; 0]; [2; 2; 0]] in
  assert_equal expected @@ Indexing.coords_of_slice slice shape;
  let slice = [R (0, -2); T 1; T (-1)] in
  let expected = [[0; 1; 2]; [1; 1; 2]] in
  assert_equal expected @@ Indexing.coords_of_slice slice shape;
  let slice = [R (1, 0); T 1; T (-1)] in
  let expected = [[1; 1; 2]; [0; 1; 2]] in
  assert_equal expected @@ Indexing.coords_of_slice slice shape;
  let slice = [I 2; I (-1); R' (-1, -1, 1)] in
  let expected = [[2; 2; 2]] in
  assert_equal expected @@ Indexing.coords_of_slice slice shape
)
;
"compute slice shape" >:: (fun _ ->
  let shape = [10; 10; 10] in
  let slice = Ndarray.Indexing.[L [0; 9; 5]; I 1; R' (2, 9, 1)] in
  assert_equal [3; 1; 8] @@ Indexing.slice_shape slice shape;
  assert_equal shape @@ Indexing.slice_shape [] shape)
;
"cartesian product" >:: (fun _ ->
  let ll = [[1; 2]; [3; 8]; [9; 4]] in
  let expected =
    [[1; 3; 9]; [1; 3; 4]; [1; 8; 9]; [1; 8; 4]
    ;[2; 3; 9]; [2; 3; 4]; [2; 8; 9]; [2; 8; 4]]
  in assert_equal expected @@ Indexing.cartesian_prod ll)
]
