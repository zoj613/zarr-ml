open OUnit2
open Zarr

let group_node = [
"group node tests" >:: (fun _ ->
  let n = Result.get_ok @@ Node.Group.(root / "somename") in
  (* test node invariants *)
  List.iter
    (fun x -> assert_equal (Error `Node_invariant) (Node.Group.create n x))
    [""; "na/me"; "...."; "__name"];
  (* creation from string path *)
  let r = Result.get_ok (Node.Group.of_path "/") in
  assert_equal ~printer:Node.Group.show Node.Group.root r;
  List.iter
    (fun x -> assert_equal (Error `Node_invariant) (Node.Group.of_path x))
    [""; "na/meas"; "/some/..."; "/root/__name"; "/sd/"];
  (* node name tests *)
  let n =  Result.get_ok (Node.Group.of_path "/some/dir/moredirs/path/pname") in
  assert_equal "pname" (Node.Group.name n);
  assert_equal "" Node.Group.(name root);
  (* parent tests *)
  assert_equal None Node.Group.(parent root);
  match Node.Group.parent n with
  | None -> assert_failure "A non-root node must have a parent.";
  | Some p -> assert_equal "/some/dir/moredirs/path" @@ Node.Group.show p;
  (* equality tests *)
  assert_equal ~printer:Node.Group.show Node.Group.root Node.Group.root;
  assert_bool "root node cannot be equal to its child" (not Node.Group.(root = n));
  assert_bool "non-root node cannot have root as child" (not Node.Group.(n = root));
  (* ancestory tests *)
  assert_equal [] Node.Group.(ancestors root);
  assert_equal
    ~printer:[%show: string list]
    ["/"; "/some"; "/some/dir"; "/some/dir/moredirs"; "/some/dir/moredirs/path"]
    (Node.Group.ancestors n |> List.map Node.Group.show);
  let exp_parents = Node.Group.ancestors n in
  let r, l = List.fold_left_map
    (fun acc _ -> match Node.Group.parent acc with
      | Some acc' -> acc', acc'
      | None -> acc, acc) n exp_parents in
  assert_equal ~printer:[%show: Node.Group.t list] exp_parents (List.rev l);
  assert_equal ~printer:Node.Group.show r Node.Group.root;
  (* child node tests *)
  let p = Node.Group.parent n |> Option.get in
  assert_equal ~printer:string_of_bool true (Node.Group.is_child_group p n);
  assert_equal ~printer:string_of_bool false Node.Group.(is_child_group n root);
  assert_equal ~printer:string_of_bool false Node.Group.(is_child_group root root);
  (* rename tests *)
  assert_equal (Error `Cannot_rename_root) Node.Group.(rename root "somename");
  assert_equal (Error `Node_invariant) Node.Group.(rename n "?illegal/");
  let n' = Result.get_ok @@ Node.Group.rename n "newname" in
  assert_bool "" Node.Group.(name n' <> name n);
  (* stringify tests *)
  assert_equal ~printer:Fun.id "" Node.Group.(to_key root);
  assert_equal ~printer:Fun.id "some/dir/moredirs/path/pname" (Node.Group.to_key n);
  assert_equal ~printer:Fun.id "zarr.json" Node.Group.(to_metakey root);
  assert_equal ~printer:Fun.id ("some/dir/moredirs/path/pname/zarr.json") (Node.Group.to_metakey n))
]

let array_node = [
"array node tests" >:: (fun _ ->
  match Node.Array.(Node.Group.root / "somename") with
  | Error _ -> assert_failure "creating '/somename' should not fail"
  | Ok _ -> ();
  (* test node invariants *)
  List.iter
    (fun x -> assert_equal (Error `Node_invariant) (Node.Array.create Node.Group.root x))
    [""; "na/me"; "...."; "__name"];
  (* creation from string path *)
  List.iter
    (fun x -> assert_equal (Error `Node_invariant) (Node.Array.of_path x))
    ["/"; ""; "na/meas"; "/some/..."; "/root/__name"; "/sd/"];
  (* node name tests *)
  let s = "/some/dir/moredirs/path/pname" in
  let n = Result.get_ok (Node.Array.of_path s) in
  assert_equal "pname" (Node.Array.name n);
  assert_equal ~printer:Fun.id s (Node.Array.show n);
  (* parent tests *)
  assert_equal
    ~printer:Node.Group.show
    Node.Group.root
    (Option.get @@ Node.Array.parent @@ (Result.get_ok @@ Node.Array.of_path "/nodename"));
  assert_equal None Node.Array.(parent root);
  (* equality tests *)
  let n' = Result.get_ok (Node.Array.of_path s) in
  assert_equal ~printer:Node.Array.show n n';
  assert_equal true Node.Array.(n = n');
  assert_equal false Node.Array.(n = Result.get_ok (of_path (s ^ "/more")));
  (* ancestory tests *)
  assert_equal [] Node.Array.(ancestors root);
  assert_equal
    ~printer:[%show: string list]
    ["/"; "/some"; "/some/dir"; "/some/dir/moredirs"; "/some/dir/moredirs/path"]
    (Node.Array.ancestors n
    |> List.map Node.Group.show
    |> List.fast_sort String.compare);
  let m = Result.get_ok (Node.Array.of_path "/some") in
  assert_equal false Node.Array.(is_parent root Node.Group.root);
  assert_equal true Node.Array.(is_parent m Node.Group.root);
  (* rename tests *)
  assert_equal (Error `Cannot_rename_root) Node.Array.(rename root "somename");
  assert_equal (Error `Node_invariant) Node.Array.(rename n "?illegal/");
  let n' = Result.get_ok (Node.Array.rename n "newname") in
  assert_bool "" Node.Array.(name n' <> name n);
  (* stringify tests *)
  assert_equal ~printer:Fun.id "some/dir/moredirs/path/pname" (Node.Array.to_key n);
  assert_equal ~printer:Fun.id "" Node.Array.(to_key root);
  assert_equal ~printer:Fun.id "/" Node.Array.(to_path root);
  assert_equal ~printer:Fun.id "zarr.json" Node.Array.(to_metakey root);
  assert_equal ~printer:Fun.id ("some/dir/moredirs/path/pname/zarr.json") (Node.Array.to_metakey n))
]

let tests = group_node @ array_node
