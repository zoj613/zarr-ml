type 'a t = 'a Lwt.t
let return = Lwt.return
let return_unit = Lwt.return_unit
let iter = Lwt_list.iter_s
let fold_left = Lwt_list.fold_left_s
let concat_map f l = Lwt.map List.concat (Lwt_list.map_s f l)

module Infix = struct
  let (>>=) = Lwt.Infix.(>>=)
  let (>>|) = Lwt.Infix.(>|=) 
end

module Syntax = struct
  let (let*) = Lwt.bind
  let (let+) x f = Lwt.map f x
end
