package rdx

import "fmt"

var __RDX_actions = []int8{0, 1, 0, 1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, 1, 8, 1, 9, 1, 10, 1, 11, 1, 12, 1, 13, 1, 14, 1, 15, 2, 1, 3, 2, 1, 10, 2, 1, 12, 2, 1, 14, 2, 1, 15, 2, 2, 10, 2, 2, 12, 2, 2, 14, 2, 2, 15, 2, 3, 10, 2, 3, 12, 2, 3, 14, 2, 3, 15, 2, 4, 10, 2, 4, 12, 2, 4, 14, 2, 4, 15, 2, 5, 10, 2, 5, 12, 2, 5, 14, 2, 5, 15, 2, 6, 10, 2, 6, 12, 2, 6, 14, 2, 6, 15, 2, 7, 10, 2, 7, 12, 2, 7, 14, 2, 7, 15, 2, 8, 10, 2, 8, 12, 2, 8, 14, 2, 8, 15, 2, 9, 10, 2, 9, 12, 2, 9, 14, 2, 9, 15, 2, 11, 0, 2, 11, 10, 2, 11, 12, 2, 11, 14, 2, 11, 15, 2, 13, 0, 2, 13, 10, 2, 13, 12, 2, 13, 14, 2, 13, 15, 3, 1, 3, 10, 3, 1, 3, 12, 3, 1, 3, 14, 3, 1, 3, 15, 0}
var __RDX_key_offsets = []int16{0, 0, 7, 11, 13, 16, 19, 21, 24, 26, 30, 34, 36, 42, 48, 55, 63, 69, 72, 81, 87, 93, 99, 105, 129, 138, 162, 171, 195, 206, 217, 231, 242, 261, 277, 292, 308, 324, 345, 361, 0}
var __RDX_trans_keys = []byte{34, 46, 57, 92, 120, 48, 49, 43, 45, 48, 57, 48, 57, 41, 48, 57, 43, 48, 57, 48, 57, 45, 48, 57, 48, 57, 69, 101, 48, 57, 43, 45, 48, 57, 48, 57, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 45, 48, 57, 65, 70, 97, 102, 43, 45, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 41, 48, 57, 34, 47, 92, 98, 102, 110, 114, 116, 117, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 58, 91, 93, 123, 125, 9, 13, 32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 58, 91, 93, 123, 125, 9, 13, 32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 58, 91, 93, 123, 125, 9, 13, 48, 57, 32, 44, 58, 91, 93, 123, 125, 9, 13, 48, 57, 32, 44, 46, 58, 69, 91, 93, 101, 123, 125, 9, 13, 48, 57, 32, 44, 58, 91, 93, 123, 125, 9, 13, 48, 57, 32, 44, 45, 46, 58, 69, 91, 93, 101, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 45, 58, 91, 93, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 58, 91, 93, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 45, 58, 91, 93, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 45, 58, 91, 93, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 90, 97, 122, 32, 44, 58, 91, 93, 123, 125, 9, 13, 0}
var __RDX_single_lengths = []int8{0, 5, 2, 0, 1, 1, 0, 1, 0, 2, 2, 0, 0, 0, 1, 2, 0, 1, 9, 0, 0, 0, 0, 12, 7, 12, 7, 12, 7, 7, 10, 7, 11, 8, 7, 8, 8, 9, 8, 7, 0}
var __RDX_range_lengths = []int8{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 3, 1, 0, 3, 3, 3, 3, 6, 1, 6, 1, 6, 2, 2, 2, 2, 4, 4, 4, 4, 4, 6, 4, 1, 0}
var __RDX_index_offsets = []int16{0, 0, 7, 11, 13, 16, 19, 21, 24, 26, 30, 34, 36, 40, 44, 49, 55, 59, 62, 72, 76, 80, 84, 88, 107, 116, 135, 144, 163, 173, 183, 196, 206, 222, 235, 247, 260, 273, 289, 302, 0}
var __RDX_cond_targs = []int8{24, 0, 0, 18, 0, 0, 1, 3, 3, 17, 0, 4, 0, 26, 4, 0, 6, 29, 0, 28, 0, 6, 30, 0, 9, 0, 10, 10, 9, 0, 11, 11, 31, 0, 31, 0, 33, 33, 33, 0, 34, 34, 34, 0, 12, 14, 14, 14, 0, 11, 16, 36, 14, 14, 0, 35, 33, 33, 0, 39, 17, 0, 1, 1, 1, 1, 1, 1, 1, 1, 19, 0, 20, 20, 20, 0, 21, 21, 21, 0, 22, 22, 22, 0, 1, 1, 1, 0, 23, 1, 2, 5, 23, 7, 23, 23, 25, 38, 23, 27, 23, 32, 37, 38, 37, 38, 0, 23, 23, 23, 23, 25, 23, 27, 23, 0, 23, 1, 2, 5, 23, 7, 23, 23, 25, 38, 23, 27, 23, 32, 37, 38, 37, 38, 0, 23, 23, 23, 23, 25, 23, 27, 23, 0, 23, 1, 2, 5, 23, 7, 23, 23, 25, 38, 23, 27, 23, 32, 37, 38, 37, 38, 0, 23, 23, 23, 23, 25, 23, 27, 23, 28, 0, 23, 23, 23, 23, 25, 23, 27, 23, 29, 0, 23, 23, 8, 23, 10, 23, 25, 10, 23, 27, 23, 30, 0, 23, 23, 23, 23, 25, 23, 27, 23, 31, 0, 23, 23, 12, 8, 23, 15, 23, 25, 15, 23, 27, 23, 32, 14, 14, 0, 23, 23, 13, 23, 23, 25, 23, 27, 23, 33, 33, 33, 0, 23, 23, 23, 23, 25, 23, 27, 23, 34, 34, 34, 0, 23, 23, 13, 23, 23, 25, 23, 27, 23, 35, 33, 33, 0, 23, 23, 12, 23, 23, 25, 23, 27, 23, 36, 14, 14, 0, 23, 23, 12, 23, 23, 25, 38, 23, 27, 23, 37, 37, 38, 37, 38, 0, 23, 23, 23, 23, 25, 38, 23, 27, 23, 38, 38, 38, 0, 23, 23, 23, 23, 25, 23, 27, 23, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 0}
var __RDX_cond_actions = []int16{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 29, 1, 31, 25, 0, 1, 21, 0, 0, 1, 1, 1, 1, 1, 0, 9, 78, 81, 75, 9, 72, 9, 9, 0, 27, 159, 159, 159, 168, 159, 171, 165, 27, 159, 162, 27, 27, 159, 159, 159, 159, 159, 0, 17, 126, 129, 123, 17, 120, 17, 17, 0, 23, 144, 144, 144, 153, 144, 156, 150, 23, 144, 147, 23, 23, 144, 144, 144, 144, 144, 0, 19, 138, 141, 135, 19, 132, 19, 19, 0, 0, 15, 114, 117, 111, 15, 108, 15, 15, 0, 0, 5, 54, 0, 57, 0, 51, 5, 0, 48, 5, 5, 0, 0, 3, 42, 45, 39, 3, 36, 3, 3, 0, 0, 5, 54, 0, 0, 57, 0, 51, 5, 0, 48, 5, 5, 0, 0, 0, 0, 7, 66, 0, 69, 63, 7, 60, 7, 7, 0, 0, 0, 0, 7, 66, 69, 63, 7, 60, 7, 7, 0, 0, 0, 0, 33, 182, 0, 186, 178, 33, 174, 33, 33, 0, 0, 0, 0, 3, 42, 0, 45, 39, 3, 36, 3, 3, 0, 0, 0, 0, 11, 90, 0, 93, 87, 11, 0, 84, 11, 11, 0, 0, 0, 0, 0, 0, 11, 90, 93, 87, 11, 0, 84, 11, 11, 0, 0, 0, 0, 13, 102, 105, 99, 13, 96, 13, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 27, 17, 23, 19, 15, 5, 3, 5, 7, 7, 33, 3, 11, 11, 13, 0}
var __RDX_eof_trans = []int16{312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 0}
var _RDX_start int = 23
var _ = _RDX_start
var _RDX_first_final int = 23
var _ = _RDX_first_final
var _RDX_error int = 0
var _ = _RDX_error
var _RDX_en_main int = 23
var _ = _RDX_en_main

func ParseRDX(data []byte) (rdx *RDX, err error) {

	var mark [RdxMaxNesting]int
	nest, cs, p, pe, eof := 0, 0, 0, len(data), len(data)

	rdx = &RDX{}

	{
		cs = int(_RDX_start)

	}
	{
		var _klen int
		var _trans uint = 0
		var _keys int
		var _acts int
		var _nacts uint
	_resume:
		{

		}
		if p == pe && p != eof {
			goto _out

		}
		if p == eof {
			if __RDX_eof_trans[cs] > 0 {
				_trans = uint(__RDX_eof_trans[cs]) - 1

			}

		} else {
			_keys = int(__RDX_key_offsets[cs])

			_trans = uint(__RDX_index_offsets[cs])
			_klen = int(__RDX_single_lengths[cs])
			if _klen > 0 {
				var _lower int = _keys
				var _upper int = _keys + _klen - 1
				var _mid int
				for {
					if _upper < _lower {
						_keys += _klen
						_trans += uint(_klen)
						break

					}
					_mid = _lower + ((_upper - _lower) >> 1)
					if (data[p]) < __RDX_trans_keys[_mid] {
						_upper = _mid - 1

					} else if (data[p]) > __RDX_trans_keys[_mid] {
						_lower = _mid + 1

					} else {
						_trans += uint((_mid - _keys))
						goto _match

					}

				}

			}
			_klen = int(__RDX_range_lengths[cs])
			if _klen > 0 {
				var _lower int = _keys
				var _upper int = _keys + (_klen << 1) - 2
				var _mid int
				for {
					if _upper < _lower {
						_trans += uint(_klen)
						break

					}
					_mid = _lower + (((_upper - _lower) >> 1) & ^1)
					if (data[p]) < __RDX_trans_keys[_mid] {
						_upper = _mid - 2

					} else if (data[p]) > __RDX_trans_keys[_mid+1] {
						_lower = _mid + 2

					} else {
						_trans += uint(((_mid - _keys) >> 1))
						break

					}

				}

			}
		_match:
			{

			}

		}
		cs = int(__RDX_cond_targs[_trans])
		if __RDX_cond_actions[_trans] != 0 {
			_acts = int(__RDX_cond_actions[_trans])

			_nacts = uint(__RDX_actions[_acts])
			_acts += 1
			for _nacts > 0 {
				switch __RDX_actions[_acts] {
				case 0:
					{
						mark[nest] = p
					}

				case 1:
					{
						rdx.RdxType = Float
						rdx.Text = data[mark[nest]:p]
					}

				case 2:
					{
						rdx.RdxType = Integer
						rdx.Text = data[mark[nest]:p]
					}

				case 3:
					{
						if rdx.RdxType != Integer {
							rdx.RdxType = Reference
						}
						rdx.Text = data[mark[nest]:p]
					}

				case 4:
					{
						rdx.RdxType = String
						rdx.Text = data[mark[nest]:p]
					}

				case 5:
					{
						rdx.RdxType = Term
						rdx.Text = data[mark[nest]:p]
					}

				case 6:
					{
						rdx.RdxType = NCounter
						rdx.Text = data[mark[nest]:p]
					}

				case 7:
					{
						rdx.RdxType = NInc
						rdx.Text = data[mark[nest]:p]
					}

				case 8:
					{
						rdx.RdxType = ZCounter
						rdx.Text = data[mark[nest]:p]
					}

				case 9:
					{
						rdx.RdxType = ZInc
						rdx.Text = data[mark[nest]:p]
					}

				case 10:
					{
						n := rdx.Nested
						n = append(n, RDX{Parent: rdx})
						rdx.Nested = n
						rdx.RdxType = Map
						rdx = &n[len(n)-1]
						nest++
					}

				case 11:
					{
						if rdx.Parent == nil {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						nest--
						rdx = rdx.Parent
						if rdx.RdxType != ESet && rdx.RdxType != Map {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						if len(rdx.Nested) == 1 {
							rdx.RdxType = ESet
						}
						if rdx.RdxType == Map {
							if (len(rdx.Nested) & 1) == 1 {
								cs = _RDX_error
								{
									p += 1
									goto _out

								}

							}
						}
					}

				case 12:
					{
						n := rdx.Nested
						n = append(n, RDX{Parent: rdx})
						rdx.Nested = n
						rdx.RdxType = LArray
						rdx = &n[len(n)-1]
						nest++
					}

				case 13:
					{
						if rdx.Parent == nil {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						nest--
						rdx = rdx.Parent
						if rdx.RdxType != LArray {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
					}

				case 14:
					{
						if rdx.Parent == nil {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						n := rdx.Parent.Nested
						if rdx.Parent.RdxType == Map {
							if len(n) == 1 {
								rdx.Parent.RdxType = ESet
							} else if (len(n) & 1) == 1 {
								cs = _RDX_error
								{
									p += 1
									goto _out

								}

							}
						}
						n = append(n, RDX{Parent: rdx.Parent})
						rdx.Parent.Nested = n
						rdx = &n[len(n)-1]
					}

				case 15:
					{
						if rdx.Parent == nil {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						n := rdx.Parent.Nested
						if rdx.Parent.RdxType == Map {
							if (len(n) & 1) == 0 {
								cs = _RDX_error
								{
									p += 1
									goto _out

								}

							}
						} else {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						n = append(n, RDX{Parent: rdx.Parent})
						rdx.Parent.Nested = n
						rdx = &n[len(n)-1]
					}

				}
				_nacts -= 1
				_acts += 1

			}

		}
		if p == eof {
			if cs >= 23 {
				goto _out

			}

		} else {
			if cs != 0 {
				p += 1
				goto _resume

			}

		}
	_out:
		{

		}

	}
	if nest != 0 || cs < _RDX_first_final {
		err = fmt.Errorf("RDX parsing failed at pos %d", p)
	}

	return
}
