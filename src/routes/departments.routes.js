const { Router } = require("express");
const {
  createDepartment,
  updateDepartment,
  deleteDepartment,
} = require("../controllers/departments.controller");

const router = Router();

router.post("/", createDepartment);
router.put("/:id", updateDepartment);
router.delete("/:id", deleteDepartment);

module.exports = router;
