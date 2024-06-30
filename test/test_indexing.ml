open OUnit2
open Zarr


let tests = [

"slice from coords" >:: (fun _ ->
  let coords =
    [[|0; 1; 2; 3|]
    ;[|9; 8; 7; 6|]
    ;[|9; 8; 7; 6|]  (* slice_of_coords should be duplicate coord-aware *)
    ;[|5; 4; 3; 2|]]
  in
  let expected =
    Owl_types.[|
    L [0; 9; 5];
    L [1; 8; 4];
    L [2; 7; 3];
    L [3; 6; 2]
  |]
  in
  assert_equal expected @@ Indexing.slice_of_coords coords;
  assert_equal [||] @@ Indexing.slice_of_coords [])
; 
"coords from slice" >:: (fun _ -> 
  let shape = [|10; 10; 10|] in
  let slice =
    Owl_types.[|L [0; 9; 5]; I 1; R [9; 3; -3]|]
  in
  let expected =
    [|[|0; 1; 9|]; [|0; 1; 6|]; [|0; 1; 3|]
    ;[|9; 1; 9|]; [|9; 1; 6|]; [|9; 1; 3|]
    ;[|5; 1; 9|]; [|5; 1; 6|]; [|5; 1; 3|]|]
  in
  assert_equal expected @@ Indexing.coords_of_slice slice shape;
  assert_equal [|[||]|] @@ Indexing.coords_of_slice [||] shape)
;
"compute slice shape" >:: (fun _ ->
  let shape = [|10; 10; 10|] in
  let slice =
    Owl_types.[|L [0; 9; 5]; I 1; R [2; 9; 1]|]
  in
  assert_equal [|3; 1; 8|] @@ Indexing.slice_shape slice shape;
  assert_equal [||] @@ Indexing.slice_shape [||] shape)
;
"cartesian product" >:: (fun _ ->
  let ll = [[1; 2]; [3; 8]; [9; 4]] in
  let expected =
    [[1; 3; 9]; [1; 3; 4]; [1; 8; 9]; [1; 8; 4]
    ;[2; 3; 9]; [2; 3; 4]; [2; 8; 9]; [2; 8; 4]]
  in
  assert_equal expected @@ Indexing.cartesian_prod ll;
  let ll = [['a'; 'b']; ['z'; 'o']] in
  let expected = [['a'; 'z']; ['a'; 'o']; ['b'; 'z']; ['b'; 'o']] in
  assert_equal expected @@ Indexing.cartesian_prod ll)
]
