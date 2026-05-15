const { Router } = require("express");
const { createOrder, updateOrder, deleteOrder } = require("../controllers/crud.controller");

const router = Router();

router.post("/", createOrder);
router.put("/:id", updateOrder);
router.delete("/:id", deleteOrder);

module.exports = router;
