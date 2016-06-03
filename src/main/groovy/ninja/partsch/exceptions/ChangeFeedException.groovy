package ninja.partsch.exceptions

class ChangeFeedException extends RuntimeException {
    def ChangeFeedException(String message = "", Throwable cause = null) {
        super(message, cause)
    }
}
