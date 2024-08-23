(lang dune 3.15)

(name zarr)

(version 0.1.0)

(generate_opam_files true)

(source
 (github zoj613/zarr-ml))

(authors "Zolisa Bleki")

(maintainers "Zolisa Bleki")

(license BSD-3-Clause)

(documentation https://zoj613.github.io/zarr-ml)

(package
 (name zarr)
 (synopsis "An Ocaml implementation of the Zarr V3 specification.")
 (description
   "The Zarr library provides an OCaml implementation of the Zarr version 3
   storage format specification for chunked & compressed multi-dimensional
   arrays, designed for use in parallel computing.")
 (depends
   dune
   (ocaml (>= 5.0.0))
   yojson
   ezgzip
   owl
   stdint
   checkseum
   domainslib
   (odoc :with-doc)
   (ounit2 :with-test)
   (ppx_deriving :with-test)
   (bisect_ppx
     (and :dev (>= 2.5.0) :with-test)))
 (tags
  ("zarr" "chunked arrays" "zarr version 3")))

(package
 (name zarr-sync)
 (synopsis "Synchronous API for Zarr.")
 (depends
   dune
   (ocaml (>= 5.0.0))
   (zarr (= :version))
   (odoc :with-doc)
   (ounit2 :with-test)
   (bisect_ppx
     (and :dev (>= 2.5.0) :with-test))))

(package
 (name zarr-lwt)
 (synopsis "Lwt-aware API for Zarr.")
 (depends
   dune
   (ocaml (>= 5.0.0))
   (zarr (= :version))
   lwt
   (odoc :with-doc)
   (ounit2 :with-test)
   (bisect_ppx
     (and :dev (>= 2.5.0) :with-test))))
