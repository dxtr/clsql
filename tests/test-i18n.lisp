;;;; -*- Mode: LISP; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;; *************************************************************************
;;;; FILE IDENTIFICATION
;;;;
;;;; Name:    test-i18n.lisp
;;;; Purpose: Tests for passing non-ascii encoded strings to db and back
;;;; Author:  Nathan Bird & Kevin M. Rosenberg
;;;; Created: Feb 2010
;;;;
;;;; This file is part of CLSQL.
;;;;
;;;; CLSQL users are granted the rights to distribute and use this software
;;;; as governed by the terms of the Lisp Lesser GNU Public License
;;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;; *************************************************************************

(in-package #:clsql-tests)

(setq *rt-i18n*
      '(

;;; The point of these two is to require proper encoding support
;;; UTF-8 for example can handle these easily.
;; I show this as a 20char string and 27 bytes in utf-8
(deftest :basic/i18n/1
 (let ((uffi:*default-foreign-encoding* :utf-8))
   (first (query "SELECT 'Iñtërnâtiônàlizætiøn'"
                 :flatp t :field-names nil)))
 "Iñtërnâtiônàlizætiøn")

;; the z in this one is even stronger
;; I show this as a 20char string and 28 bytes in utf-8
(deftest :basic/i18n/2
 (let ((uffi:*default-foreign-encoding* :utf-8))
   (first (query "SELECT 'Iñtërnâtiônàližætiøn'"
                 :flatp t :field-names nil)))
 "Iñtërnâtiônàližætiøn")

(deftest :basic/i18n/big/1
    (let ((test-string (with-output-to-string (str)
                         (dotimes (n 250)
                           (write-sequence "Iñtërnâtiônàližætiøn" str)))))
      (with-dataset *ds-bigtext*
        (clsql-sys:execute-command
         (format nil
                 "INSERT INTO testbigtext (a) VALUES ('~a')"
                 test-string))
        (let ((res (first (clsql:query "SELECT a from testbigtext" :flatp t :field-names nil))))
          (assert (equal test-string res) (test-string res)
                  "Returned internationalization string was incorrect. Test :basic/i18n/big/1")))))

))
