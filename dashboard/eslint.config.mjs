import { dirname } from "path";
import { fileURLToPath } from "url";
import { FlatCompat } from "@eslint/eslintrc";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const compat = new FlatCompat({
    baseDirectory: __dirname,
});

const eslintConfig = [
    ...compat.extends("next/core-web-vitals", "next/typescript"),
    {
        rules: {
            'indent': ['error', 4], // Sets indentation to 4 spaces
            'semi': ['error', 'always'],
            'quotes': ['error', 'single']
        },
    },
];

export default eslintConfig;
