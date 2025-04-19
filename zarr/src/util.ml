module CoordMap = struct
  include Map.Make (struct
    type t = int list
    let compare : t -> t -> int = Stdlib.compare
  end)

  let add_to_list k v map =
    let f ~v = function
      | None -> Some [v]
      | Some l -> Some (v :: l)
    in
    update k (f ~v) map
end

module Result_syntax = struct
  let both x y = match x, y with
    | Ok a, Ok b -> Ok (a, b)
    | Error _ as r, _ -> r
    | _, (Error _ as r) -> r
  let (let*) = Result.bind
  let (and*) = both
  let (let+) x f = Result.map f x
  let (and+) = both
end

let get_name j = Yojson.Safe.Util.(member "name" j |> to_string)
let max = Array.fold_left Int.max Int.min_int

(* Obtained from: https://discuss.ocaml.org/t/how-to-create-a-new-file-while-automatically-creating-any-intermediate-directories/14837/5?u=zoj613 *)
let rec create_parent_dir fn perm =
  let parent_dir = Filename.dirname fn in
  if not (Sys.file_exists parent_dir) then begin
    create_parent_dir parent_dir perm;
    Sys.mkdir parent_dir perm
  end

let sanitize_dir dir = match Filename.chop_suffix_opt ~suffix:"/" dir with
  | None -> dir
  | Some d -> d

let rec cartesian_prod : int list list -> int list list = function
  | [] -> [[]]
  | x :: xs -> List.concat_map (fun i -> List.map (List.cons i) (cartesian_prod xs)) x
