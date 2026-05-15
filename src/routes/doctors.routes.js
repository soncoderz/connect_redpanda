const { Router } = require("express");
const {
  createDoctor,
  updateDoctor,
  deleteDoctor,
} = require("../controllers/doctors.controller");

const router = Router();

router.post("/", createDoctor);
router.put("/:id", updateDoctor);
router.delete("/:id", deleteDoctor);

module.exports = router;
