package org.komamitsu.wormhole;

class Tuple<F, S> {
  final F first;
  final S second;

  Tuple(F first, S second) {
    this.first = first;
    this.second = second;
  }
}
