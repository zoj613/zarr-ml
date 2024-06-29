type t =
  | Root
  | Cons of t * name
and name = string

type error =
  [ `Node_invariant_error of string ]

(* Check if the path's name satisfies path invariants *)
let rep_ok name =
  (String.empty <> name) &&
  not (String.contains name '/') &&
  not (String.for_all (Char.equal '.') name) &&
  not (String.starts_with ~prefix:"__" name)

let root = Root 

let create parent name =
  if rep_ok name then
    Result.ok @@ Cons (parent, name)
  else
    Error (`Node_invariant_error name)

let ( / ) = create

let of_path = function
  | "/" -> Ok Root
  | str ->
    if not String.(starts_with ~prefix:"/" str) then
      Result.error @@
      `Node_invariant_error "path should start with a /"
    else if String.ends_with ~suffix:"/" str then
      Result.error @@
      `Node_invariant_error "path should not end with a /"
    else 
      let open Util.Result_syntax in
      List.fold_left
        (fun acc n -> acc >>= fun p -> create p n)
        (Ok Root) (List.tl @@ String.split_on_char '/' str)

let name = function
  | Root -> ""
  | Cons (_, n) -> n

let parent = function
  | Root -> None
  | Cons (parent, _) -> Some parent

let rec fold f acc = function
  | Root -> f acc Root
  | Cons (parent, _) as p ->
    fold f (f acc p) parent

let rec ( = ) x y =
  match x, y with
  | Root, Root -> true
  | Root, Cons _ | Cons _, Root -> false
  | Cons (p, n), Cons (q, m) -> ( = ) p q && String.equal n m

let to_path = function
  | Root -> "/"
  | p ->
    fold (fun acc -> function
      | Root -> acc
      | Cons (_, n) -> "/" :: n :: acc) [] p
    |> String.concat ""

let ancestors p =
  fold (fun acc -> function
    | Root -> acc
    | Cons (parent, _) -> parent :: acc) [] p

let to_key p =
  let str = to_path p in
  String.(length str - 1 |> sub str 1)

let to_prefix = function
  | Root -> ""
  | p -> to_key p ^ "/"

let to_metakey p =
  to_prefix p ^ "zarr.json"

let is_parent x y =
  match x, y with
  | Root, _ -> false
  | Cons (parent, _), v -> parent = v
