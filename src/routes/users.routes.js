const { Router } = require("express");
const { createUser, updateUser, deleteUser } = require("../controllers/crud.controller");

const router = Router();

router.post("/", createUser);
router.put("/:id", updateUser);
router.delete("/:id", deleteUser);

module.exports = router;
