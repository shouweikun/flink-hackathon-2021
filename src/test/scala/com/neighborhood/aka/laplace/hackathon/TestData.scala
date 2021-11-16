/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon

object TestData {

  val BULK_DATA = List(
    ("i", 1, 2),
    ("i", 2, 1),
    ("i", 3, 3),
    ("i", 4, 4),
    ("i", 7, 1),
    ("i", 7, 1),
    ("i", 8, 1),
    ("i", 9, 1),
    ("i", 10, 1),
    ("i", 11, 1),
    ("i", 12, 1)
  )

  val CHANGELOG_DATA = List(
    ("i", 1, 1, 1637046364900L),
    ("i", 2, 1, 1637046364901L),
    ("i", 3, 1, 1637046364902L),
    ("-u", 1, 1, 1637046364903L),
    ("+u", 1, 2, 1637046364904L),
    ("-u", 2, 1, 1637046364905L),
    ("+u", 2, 2, 1637046364906L),
    ("-u", 3, 1, 1637046364908L),
    ("+u", 3, 4, 1637046364909L),
    ("i", 5, 5, 1637046364910L),
    // duplicate
    ("+u", 1, 2, 1637046364904L),
    ("-u", 2, 1, 1637046364905L),
    ("+u", 2, 2, 1637046364906L),
    ("i", 5, 5, 1637046364910L),
    // duplicate
    ("d", 1, 2, 2637046364911L),
    ("i", 6, 1, 2637046364912L)
  )

}
