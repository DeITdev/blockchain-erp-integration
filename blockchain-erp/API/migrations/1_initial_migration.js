// This deploys the Migrations contract that tracks all other migrations
const Migrations = artifacts.require("Migrations");

module.exports = function (deployer) {
  deployer.deploy(Migrations);
};