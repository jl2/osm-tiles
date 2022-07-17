;; osm-tiles.lisp
;;
;; Copyright (c) 2022 Jeremiah LaRocco <jeremiah_larocco@fastmail.com>


;; Permission to use, copy, modify, and/or distribute this software for any
;; purpose with or without fee is hereby granted, provided that the above
;; copyright notice and this permission notice appear in all copies.

;; THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
;; WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
;; MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
;; ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
;; WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
;; ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
;; OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

(in-package :osm-tiles)


(defparameter *cache-directory* (asdf:system-relative-pathname :osm-tiles "tile-cache/"))
(defparameter *tile-base-url* "http://localhost:8080/tile")

(defun clear-cache ()
  "Remove the entire tile-cache directory."
  (uiop/filesystem:delete-directory-tree (asdf:system-relative-pathname :osm-tiles "tile-cache/")))

(defun cached-tile-filename (z x y)
  "Return the cache file name for tile z, x, y"
  (asdf:system-relative-pathname :osm-tiles
                                 (format nil "tile-cache/~a/~a/~a.png" z x y)))
(defun is-tile-cached (z x y)
  "Check the cache for tile z,x, y."
  (probe-file (cached-tile-filename z x y)))

(defun tile-url (z x y)
  "Return the URL of tile z x y."
  (format nil "~a/~a/~a/~a.png" *tile-base-url* z x y))

(defun fetch-tile-zxy (z x y)
  (let ((tile-file (is-tile-cached z x y)))
    (unless tile-file
      (dex:fetch (tile-url z x y) (cached-tile-filename z x y))))
  (is-tile-cached z x y))

(defun fetch-tile-lat-lon (zoom lat lon)
  (multiple-value-call
      #'fetch-tile-zxy
    zoom
    (multiple-value-call
        #'gmt:meters-to-tile
      zoom
      (gmt:lat-lon-to-meters lat lon))))

(defun get-tile-zxy (z x y)
  (if-let ((fpath (is-tile-cached z x y)))
    (with-input-from-file (inf fpath)
      (png:decode inf :preserve-alpha t))
    (png:decode (flexi-streams:make-in-memory-input-stream
                 (dex:get (tile-url z x y)))
                :preserve-alpha t)))

(defun get-tile-lat-lon (zoom lat lon)
  (multiple-value-call #'get-tile-zxy
    zoom
    (multiple-value-call #'gmt:meters-to-tile
      zoom
      (gmt:lat-lon-to-meters lat lon))))

;; Connect to PostGIS
(defmacro q (query &rest args)
  `(postmodern:query ,query ,@args))

(defmacro sq (query &rest args)
  (let ((column (gensym))
        (results (gensym)))
    `(let ((,results (postmodern:query ,query ,@args)))
       (dolist (,column ,results)
         (format t "~{~40a ~^|~}~%" ,column))
       ,results)))

(defun describe-table (tname)
  (sq "select table_name, column_name, data_type from information_schema.columns where table_name = $1" tname))

(defun show-tables (schema)
  (sq "select table_catalog, table_name, table_type from information_schema.tables where table_schema = $1;" schema))

(defun co-connect ()
  (postmodern:connect-toplevel "gis" "renderer" "renderer" "localhost" :port 54321)
  (q "SET search_path TO import,public;"))

(defun disconnect ()
  (postmodern:disconnect-toplevel))
(defun show-indices ()
  (sq "SELECT relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch FROM pg_stat_all_indexes WHERE
   schemaname = 'public';"))

(setf lparallel:*kernel* (lparallel:make-kernel 22))
(defun prefetch-tiles (zoom &key
                              (min-lat 37.0) (min-lon -109.0)
                              (max-lat 41.0) (max-lon -102.0))
  (let ((channel (lparallel:make-channel)))
    (multiple-value-bind (minxt minyt maxxt maxyt) (gmt:lat-lon-tile-boundary
                                                    (make-instance 'gmt:global-mercator)
                                                    zoom
                                                    min-lat min-lon max-lat max-lon)
      (loop
        :for x :from minxt :to maxxt
        :do
           (loop
             :for y :from minyt :to maxyt
             :do
                (lparallel:submit-task channel (lambda (z x y) (let ((tile (fetch-tile-zxy z x y)))
                                                                 (sleep 0.0125)
                                                                 tile))
                                       zoom x y)))
      (loop
        :for x :from minxt :to maxxt
        :do
           (loop
             :for y :from minyt :to maxyt
             :do
                (lparallel:receive-result channel))))))
