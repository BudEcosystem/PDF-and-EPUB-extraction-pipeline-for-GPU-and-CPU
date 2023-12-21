const express = require('express')
const { MathMLToLaTeX } = require('mathml-to-latex');

const app = express();

app.use(express.json())
const port = 9007;

// Define a route
app.post('/', (req, res) => {
    try {
        latex = MathMLToLaTeX.convert(req.body.math_tag);
        console.log(latex)
        res.send({
            status: "success",
            data: latex
        })
    } catch (error) {
        res.send({
            status: "failed",
            error: error
        })
    }
});

// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});