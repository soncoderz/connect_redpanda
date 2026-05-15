const { Router } = require("express");
const { publishToTopic } = require("../controllers/publish.controller");

const router = Router();

router.post("/:topic", publishToTopic);

module.exports = router;
