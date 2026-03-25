import unittest

from parsers.jcsales import _parse_with_patterns


class JCSalesParserLineTests(unittest.TestCase):
    def test_parses_single_letter_type_code(self):
        row = _parse_with_patterns(
            "28 C 122590 RICO BUBBLE MILK TEA STRAWBERRY DRINK 12.3 OZ 1 1 PK 24 24 0.79 18.96 18.96",
            0,
        )

        self.assertIsNotNone(row)
        self.assertEqual("122590", row["ITEM"])
        self.assertEqual("RICO BUBBLE MILK TEA STRAWBERRY DRINK 12.3 OZ", row["DESCRIPTION"])
        self.assertEqual(24, row["PACK"])
        self.assertEqual(18.96, row["COST"])
        self.assertEqual(0.79, row["UNIT"])

    def test_parses_two_letter_type_code(self):
        row = _parse_with_patterns(
            "29 TC 131237 RED BULL ENERGY DRINK COCONUT 8.4 OZ 1 1 PK 24 24 1.89 45.36 45.36",
            0,
        )

        self.assertIsNotNone(row)
        self.assertEqual("131237", row["ITEM"])
        self.assertEqual("RED BULL ENERGY DRINK COCONUT 8.4 OZ", row["DESCRIPTION"])
        self.assertEqual(24, row["PACK"])
        self.assertEqual(45.36, row["COST"])
        self.assertEqual(1.89, row["UNIT"])

    def test_parses_multi_quantity_two_letter_type_code(self):
        row = _parse_with_patterns(
            "1 TC 117005 MINERAGUA PLASTIC BOTTLE 17.7 OZ 6 6 PK 24 144 1.05 25.20 151.20",
            0,
        )

        self.assertIsNotNone(row)
        self.assertEqual("117005", row["ITEM"])
        self.assertEqual("MINERAGUA PLASTIC BOTTLE 17.7 OZ", row["DESCRIPTION"])
        self.assertEqual(24, row["PACK"])
        self.assertEqual(25.20, row["COST"])
        self.assertEqual(1.05, row["UNIT"])


if __name__ == "__main__":
    unittest.main()
