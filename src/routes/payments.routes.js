const { Router } = require("express");
const { createPayment, updatePayment, deletePayment } = require("../controllers/crud.controller");

const router = Router();

router.post("/", createPayment);
router.put("/:id", updatePayment);
router.delete("/:id", deletePayment);

module.exports = router;
